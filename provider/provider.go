package provider

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/api"
	"github.com/filswan/swan-boost-lib/client"
	myask "github.com/filswan/swan-boost-lib/storedask"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"net/http"
	"strconv"
	"time"

	"github.com/docker/go-units"
	boostapi "github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	chain_type "github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

type Client struct {
	stub boostapi.BoostStruct
}

func NewClient(authToken, apiUrl string) (*Client, jsonrpc.ClientCloser, error) {
	var headers http.Header
	if authToken != "" {
		headers = http.Header{"Authorization": []string{"Bearer " + authToken}}
	} else {
		headers = nil
	}

	var apiSub boostapi.BoostStruct
	closer, err := jsonrpc.NewMergeClient(context.Background(), "ws://"+apiUrl+"/rpc/v0", "Filecoin",
		[]interface{}{&apiSub.Internal}, headers)
	if err != nil {
		return nil, nil, errors.Wrap(err, "connecting with boost failed")
	}

	return &Client{
		stub: apiSub,
	}, closer, nil
}

func (pc *Client) OfflineDealWithData(ctx context.Context, dealUuid, filePath string, isDelete bool) (*DealRejectionInfo, error) {
	dealUid, err := uuid.Parse(dealUuid)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("dealUuid=[%s] parse failed", dealUid))
	}
	offlineDealWithData, err := pc.stub.BoostOfflineDealWithData(ctx, dealUid, filePath, isDelete)
	if err != nil {
		return nil, err
	}
	return &DealRejectionInfo{
		Accepted: offlineDealWithData.Accepted,
		Reason:   offlineDealWithData.Reason,
	}, nil
}

func (pc *Client) MarketSetAsk(ctx context.Context, boostRepo string, fullNode api.FullNode, minerId string, price, verifiedPrice, minPieceSize, maxPieceSize string) error {
	pri, err := chain_type.ParseFIL(price)
	if err != nil {
		return err
	}

	vpri, err := chain_type.ParseFIL(verifiedPrice)
	if err != nil {
		return err
	}

	min, err := units.RAMInBytes(minPieceSize)
	if err != nil {
		return xerrors.Errorf("cannot parse min-piece-size to quantity of bytes: %w", err)
	}

	if min < 256 {
		return xerrors.New("minimum piece size (w/bit-padding) is 256B")
	}

	max, err := units.RAMInBytes(maxPieceSize)
	if err != nil {
		return xerrors.Errorf("cannot parse max-piece-size to quantity of bytes: %w", err)
	}
	dur, err := time.ParseDuration("720h0m0s")
	if err != nil {
		return xerrors.Errorf("cannot parse duration: %w", err)
	}

	qty := dur.Seconds() / float64(build.BlockDelaySecs)

	miner, err := address.NewFromString(minerId)
	if err != nil {
		return fmt.Errorf("converting miner ID from config: %w", err)
	}

	var opts []legacytypes.StorageAskOption
	opts = append(opts, legacytypes.MinPieceSize(abi.PaddedPieceSize(min)))
	opts = append(opts, legacytypes.MaxPieceSize(abi.PaddedPieceSize(max)))

	storedAsk, err := myask.NewStoredAsk(boostRepo, fullNode)
	return storedAsk.SetAsk(ctx, chain_type.BigInt(pri), chain_type.BigInt(vpri), abi.ChainEpoch(qty), miner, opts...)
}

func (pc *Client) CheckBoostStatus(ctx context.Context) (peer.ID, error) {
	return pc.stub.ID(ctx)
}

func (pc *Client) BoostDirectDeal(ctx context.Context, boostRepo string, fullNodeUrl string, walletAddress string, allocationId string, filepath string, piececidStr string, isDelete bool) (*DealRejectionInfo, error) {
	myClient, err := client.GetClient(boostRepo).WithUrl(fullNodeUrl)
	if err != nil {
		return nil, err
	}

	fullNodeApi, lcloser, err := myClient.GetLotusFullNodeApi()
	if err != nil {
		return nil, err
	}
	defer lcloser()

	head, err := fullNodeApi.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chain head: %w", err)
	}

	clientAddr, err := address.NewFromString(walletAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clientaddr param: %w", err)
	}

	piececid, err := cid.Decode(piececidStr)
	if err != nil {
		return nil, fmt.Errorf("could not parse piececid: %w", err)
	}

	allocationIdUnit, err := strconv.ParseUint(allocationId, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse allocationId param: %w", err)
	}
	startEpoch := head.Height() + (builtin.EpochsInDay * 2)

	alloc, err := fullNodeApi.StateGetAllocation(ctx, clientAddr, verifreg.AllocationId(allocationIdUnit), head.Key())
	if err != nil {
		return nil, fmt.Errorf("getting claim details from chain: %w", err)
	}

	if alloc.Expiration < startEpoch {
		return nil, fmt.Errorf("allocation will expire on %d before start epoch %d", alloc.Expiration, startEpoch)
	}

	// Since StartEpoch is more than Head+StartEpochSealingBuffer, we can set end epoch as start+TermMin
	endEpoch := startEpoch + alloc.TermMin

	ddParams := types.DirectDealParams{
		DealUUID:           uuid.New(),
		AllocationID:       verifreg.AllocationId(allocationIdUnit),
		PieceCid:           piececid,
		ClientAddr:         clientAddr,
		StartEpoch:         startEpoch,
		EndEpoch:           endEpoch,
		FilePath:           filepath,
		DeleteAfterImport:  isDelete,
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	directDeal, err := pc.stub.BoostDirectDeal(ctx, ddParams)
	if err != nil {
		return nil, err
	}
	return &DealRejectionInfo{
		Accepted: directDeal.Accepted,
		Reason:   directDeal.Reason,
	}, nil
}

func statusMessage(resp *types.DealStatusResponse) string {
	switch resp.DealStatus.Status {
	case dealcheckpoints.Accepted.String():
		if resp.IsOffline {
			return "Awaiting Offline Data Import"
		}
	case dealcheckpoints.Transferred.String():
		return "Ready to Publish"
	case dealcheckpoints.Published.String():
		return "Awaiting Publish Confirmation"
	case dealcheckpoints.PublishConfirmed.String():
		return "Adding to Sector"
	case dealcheckpoints.AddedPiece.String():
		return "Announcing"
	case dealcheckpoints.IndexedAndAnnounced.String():
		return "Sealing"
	case dealcheckpoints.Complete.String():
		if resp.DealStatus.Error != "" {
			return "Error: " + resp.DealStatus.Error
		}
		return "Expired"
	}
	return resp.DealStatus.Status
}
