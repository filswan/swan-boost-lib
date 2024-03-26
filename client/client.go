package client

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mbig "math/big"
	"net/url"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost-gfm/storagemarket/network"
	clinode "github.com/filecoin-project/boost/cli/node"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	apiclient "github.com/filecoin-project/lotus/api/client"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/filswan/go-swan-lib/client/lotus"
	"github.com/filswan/go-swan-lib/constants"
	"github.com/filswan/go-swan-lib/logs"
	"github.com/filswan/go-swan-lib/model"
	"github.com/filswan/go-swan-lib/utils"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	inet "github.com/libp2p/go-libp2p/core/network"
	"github.com/mitchellh/go-homedir"
	"github.com/shopspring/decimal"
	"github.com/urfave/cli/v2"
)

const (
	DealProtocolv120 = "/fil/storage/mk/1.2.0"
	AskProtocolID    = "/fil/storage/ask/1.1.0"
)

type Client struct {
	lotus       *lotus.LotusClient
	FullNodeApi string
	ClientRepo  string
}

func (client *Client) WithUrl(fullNodeApi string) (*Client, error) {
	client.FullNodeApi = fullNodeApi
	apiInfo := cliutil.ParseApiInfo(client.FullNodeApi)
	addr, err := apiInfo.DialArgs("v1")
	if err != nil {
		logs.GetLogger().Error("parse fullNodeApi failed: %w", err)
		return nil, err
	}
	client.lotus = &lotus.LotusClient{
		ApiUrl:      addr,
		AccessToken: string(apiInfo.Token),
	}
	return client, nil
}

func (client *Client) WithClient(lotus *lotus.LotusClient) *Client {
	client.lotus = lotus
	u, err := url.Parse(lotus.ApiUrl)
	if err != nil {
		logs.GetLogger().Error("parse lotus ApiUrl failed: %w", err)
		return nil
	}
	client.FullNodeApi = fmt.Sprintf("%s:/ip4/%s/tcp/%s/http", lotus.AccessToken, u.Hostname(), u.Port())
	return client
}

func (client *Client) WithRepo(clientRepo string) *Client {
	if len(clientRepo) == 0 {
		panic("boost repo is required")
	}
	_, err := os.Stat(clientRepo)
	if err != nil {
		panic(err)
	}
	client.ClientRepo = clientRepo
	return client
}

func GetClient(clientRepo string) *Client {
	if len(clientRepo) == 0 {
		panic("boost repo is required")
	}
	return &Client{
		ClientRepo: clientRepo,
	}
}

func (client *Client) InitRepo(repoPath, walletAddress string) error {
	sdir, err := homedir.Expand(repoPath)
	if err != nil {
		return err
	}
	os.Mkdir(sdir, 0755) //nolint:errcheck

	_, err = clinode.Setup(repoPath)
	if err != nil {
		logs.GetLogger().Error("setup node failed: %w", err)
		return err
	}

	fmt.Println(color.YellowString("The current client wallet address is: %s, please use the command <./swan-client wallet import wallet.key> to import the wallet private key.", walletAddress))
	fmt.Println(color.YellowString("You must add funds to it in order to send deals. please run `lotus wallet market add --from <address> --address <market_address> <amount>"))
	return nil
}

func (client *Client) ValidateExistWalletAddress(walletAddress string) bool {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		logs.GetLogger().Error("setup node failed: %w", err)
		return false
	}

	addressList, err := n.Wallet.WalletList(ctx)
	if err != nil {
		logs.GetLogger().Error("wallet list failed: %w", err)
		return false
	}

	for _, addr := range addressList {
		if strings.EqualFold(addr.String()[1:], walletAddress[1:]) {
			return true
		}
	}
	return false
}

func (client *Client) WalletImport(inputData []byte) error {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		logs.GetLogger().Error("setup node failed: %w", err)
		return err
	}

	var ki chaintypes.KeyInfo
	data, err := hex.DecodeString(strings.TrimSpace(string(inputData)))
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, &ki); err != nil {
		return err
	}

	_, err = n.Wallet.WalletImport(ctx, &ki)
	if err != nil {
		logs.GetLogger().Error("wallet import failed: %w", err)
		return err
	}
	logs.GetLogger().Infof("wallet import successfully")
	return nil
}

func (client *Client) WalletNew(walletType string) error {
	ctx := context.Background()

	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		return err
	}
	var t string
	if walletType == "" {
		t = constants.WALLET_TYPE_256
	} else {
		t = walletType
	}

	if walletType != constants.WALLET_TYPE_256 && walletType != constants.WALLET_TYPE_BLS {
		return errors.New("only support walletType: secp256k1 or bls")
	}
	nk, err := n.Wallet.WalletNew(ctx, chaintypes.KeyType(t))
	if err != nil {
		return err
	}
	fmt.Println("address: ", nk.String())
	return nil
}

func (client *Client) WalletList() error {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		logs.GetLogger().Error("setup node failed: %w", err)
		return err
	}

	addressList, err := n.Wallet.WalletList(ctx)
	if err != nil {
		logs.GetLogger().Error("wallet list failed: %w", err)
		return err
	}

	ainfo := cliutil.ParseApiInfo(client.FullNodeApi)
	addr, err := ainfo.DialArgs("v1")
	if err != nil {
		logs.GetLogger().Error("parse fullNodeApi failed: %w", err)
		return err
	}

	fullNodeApi, closer, err := apiclient.NewFullNodeRPCV1(context.Background(), addr, ainfo.AuthHeader())
	if err != nil {
		return fmt.Errorf("cant setup gateway connection: %w", err)
	}
	defer closer()

	// Map Keys. Corresponds to the standard tablewriter output
	addressKey := "Address"
	idKey := "ID"
	balanceKey := "Balance"
	marketAvailKey := "Market(Avail)"
	marketLockedKey := "Market(Locked)"
	nonceKey := "Nonce"
	errorKey := "Error"
	dataCapKey := "DataCap"

	var wallets []map[string]interface{}
	for _, addr := range addressList {
		a, err := fullNodeApi.StateGetActor(ctx, addr, chaintypes.EmptyTSK)
		if err != nil {
			if !strings.Contains(err.Error(), "actor not found") {
				wallet := map[string]interface{}{
					addressKey: addr,
					errorKey:   err,
				}
				wallets = append(wallets, wallet)
				continue
			}

			a = &chaintypes.Actor{
				Balance: big.Zero(),
			}
		}

		wallet := map[string]interface{}{
			addressKey: addr,
			balanceKey: chaintypes.FIL(a.Balance),
			nonceKey:   a.Nonce,
		}

		id, err := fullNodeApi.StateLookupID(ctx, addr, chaintypes.EmptyTSK)
		if err != nil {
			wallet[idKey] = "n/a"
		} else {
			wallet[idKey] = id
		}

		mbal, err := fullNodeApi.StateMarketBalance(ctx, addr, chaintypes.EmptyTSK)
		if err == nil {
			marketAvailValue := chaintypes.FIL(chaintypes.BigSub(mbal.Escrow, mbal.Locked))
			marketLockedValue := chaintypes.FIL(mbal.Locked)
			wallet[marketAvailKey] = marketAvailValue
			wallet[marketLockedKey] = marketLockedValue
		}

		dcap, err := fullNodeApi.StateVerifiedClientStatus(ctx, addr, chaintypes.EmptyTSK)
		if err == nil {
			wallet[dataCapKey] = dcap
			if dcap == nil {
				wallet[dataCapKey] = "X"
			}
		} else {
			wallet[dataCapKey] = "n/a"
		}

		wallets = append(wallets, wallet)
	}

	// Init the tablewriter's columns
	tw := tablewriter.New(
		tablewriter.Col(addressKey),
		tablewriter.Col(idKey),
		tablewriter.Col(balanceKey),
		tablewriter.Col(marketAvailKey),
		tablewriter.Col(marketLockedKey),
		tablewriter.Col(nonceKey),
		tablewriter.NewLineCol(errorKey))
	// populate it with content
	for _, wallet := range wallets {
		tw.Write(wallet)
	}
	// return the corresponding string
	return tw.Flush(os.Stdout)
}

func (client *Client) WalletExport(walletAddress string) error {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		logs.GetLogger().Error("setup node failed: %w", err)
		return err
	}

	addr, err := address.NewFromString(walletAddress)
	if err != nil {
		return err
	}
	ki, err := n.Wallet.WalletExport(ctx, addr)
	if err != nil {
		return err
	}

	b, err := json.Marshal(ki)
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(b))
	return nil
}

func (client *Client) WalletDelete(walletAddress string) error {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		logs.GetLogger().Error("setup node failed: %w", err)
		return err
	}

	addr, err := address.NewFromString(walletAddress)
	if err != nil {
		return err
	}

	return n.Wallet.WalletDelete(ctx, addr)
}

func (client *Client) AllocateDeal(dealConfig *model.DealConfig, wallet string) (id uint64, err error) {
	pieceSize, _ := utils.CalculatePieceSize(dealConfig.FileSize, true)
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		return
	}
	apiInfo := cliutil.ParseApiInfo(client.FullNodeApi)
	gapi, closer, err := apiclient.NewGatewayRPCV1(ctx, apiInfo.Addr, apiInfo.AuthHeader())
	defer closer()

	walletAddr, err := address.NewFromString(wallet)
	if err != nil {
		return
	}

	msg, err := util.CreateAllocationMsg(ctx, gapi, []string{fmt.Sprintf("%s=%s", dealConfig.PayloadCid, pieceSize)}, []string{dealConfig.MinerFid}, walletAddr, verifregst.MinimumVerifiedAllocationTerm, verifregst.MaximumVerifiedAllocationTerm, abi.ChainEpoch(dealConfig.Duration))
	if err != nil {
		return
	}

	oldallocations, err := gapi.StateGetAllocations(ctx, walletAddr, chaintypes.EmptyTSK)
	if err != nil {
		return 0, fmt.Errorf("failed to get allocations: %w", err)
	}
	cctx := cli.NewContext(nil, new(flag.FlagSet), nil)
	cctx.Set("assume-yes", "true")
	mcid, sent, err := lib.SignAndPushToMpool(cctx, ctx, gapi, n, msg)
	if err != nil {
		return
	}
	if !sent {
		return 0, errors.New("send failed")
	}

	logs.GetLogger().Infof("submitted data cap allocation message", "cid", mcid.String())
	logs.GetLogger().Info("waiting for message to be included in a block")

	res, err := gapi.StateWaitMsg(ctx, mcid, 1, lapi.LookbackNoLimit, true)
	if err != nil {
		return 0, fmt.Errorf("waiting for message to be included in a block: %w", err)
	}

	if !res.Receipt.ExitCode.IsSuccess() {
		return 0, fmt.Errorf("failed to execute the message with error: %s", res.Receipt.ExitCode.Error())
	}

	newallocations, err := gapi.StateGetAllocations(ctx, walletAddr, chaintypes.EmptyTSK)
	if err != nil {
		return 0, fmt.Errorf("failed to get allocations: %w", err)
	}

	// Generate a diff to find new allocations
	for i := range newallocations {
		_, ok := oldallocations[i]
		if ok {
			delete(newallocations, i)
		}
	}

	for aid, allocation := range newallocations {
		if allocation.Data.String() == dealConfig.PieceCid {
			return uint64(aid), nil
		}
	}
	return 0, errors.New("not found allocation")
}

func (client *Client) StartDeal(dealConfig *model.DealConfig) (string, error) {
	minerPrice, _, err := ValidateDealConfig(client.lotus, dealConfig, true)
	if err != nil {
		return "", err
	}
	pieceSize, sectorSize := utils.CalculatePieceSize(dealConfig.FileSize, true)
	cost := utils.CalculateRealCost(sectorSize, *minerPrice)
	epochPrice := *cost.Mul(decimal.NewFromFloat(constants.LOTUS_PRICE_MULTIPLE_1E18)).BigInt()
	return client.StartDealDirect(pieceSize, epochPrice, dealConfig)
}

func (client *Client) StartDealDirect(pieceSize int64, epochPrice mbig.Int, dealConfig *model.DealConfig) (string, error) {
	if !dealConfig.SkipConfirmation {
		logs.GetLogger().Info("Do you confirm to submit the deal?")
		logs.GetLogger().Info("Press Y/y to continue, other key to quit")
		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			logs.GetLogger().Error(err)
			return "", err
		}

		response = strings.TrimRight(response, "\n")

		if !strings.EqualFold(response, "Y") {
			logs.GetLogger().Info("Your input is ", response, ". Now give up submit the deal.")
			return "", err
		}
	}

	dealConfig.PieceCid = strings.Trim(dealConfig.PieceCid, " ")

	dealParam := DealParam{
		Provider:      dealConfig.MinerFid,
		Commp:         dealConfig.PieceCid,
		PieceSize:     uint64(pieceSize),
		CarSize:       uint64(dealConfig.FileSize),
		PayloadCid:    dealConfig.PayloadCid,
		StartEpoch:    int(dealConfig.StartEpoch),
		Duration:      dealConfig.Duration,
		StoragePrice:  int(epochPrice.Int64()),
		Verified:      dealConfig.VerifiedDeal,
		FastRetrieval: dealConfig.FastRetrieval,
		Wallet:        dealConfig.SenderWallet,
	}

	dealUuid, err := client.sendDealToMiner(dealParam)
	if err != nil {
		logs.GetLogger().Error(err)
		return "", err
	}
	return dealUuid, nil
}

func (client *Client) sendDealToMiner(dealP DealParam) (string, error) {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		return "", err
	}
	defer n.Host.Close()

	ainfo := cliutil.ParseApiInfo(client.FullNodeApi)
	addr, err := ainfo.DialArgs("v1")
	if err != nil {
		logs.GetLogger().Error("parse fullNodeApi failed: %w", err)
		return "", err
	}

	fullNode, closer, err := apiclient.NewFullNodeRPCV1(context.Background(), addr, ainfo.AuthHeader())
	if err != nil {
		return "", fmt.Errorf("cant setup fullnode connection: %w", err)
	}
	defer closer()

	walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, dealP.Wallet)
	if err != nil {
		return "", err
	}

	logs.GetLogger().Warn("selected wallet: ", walletAddr)

	maddr, err := address.NewFromString(dealP.Provider)
	if err != nil {
		return "", err
	}

	addrInfo, err := cmd.GetAddrInfo(ctx, fullNode, maddr)
	if err != nil {
		return "", err
	}

	logs.GetLogger().Warn("found storage provider, ", "id: ", addrInfo.ID, ", multiaddrs: ", addrInfo.Addrs, ", minerID:", maddr)

	if err := n.Host.Connect(ctx, *addrInfo); err != nil {
		return "", fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
	}

	x, err := n.Host.Peerstore().FirstSupportedProtocol(addrInfo.ID, DealProtocolv120)
	if err != nil {
		return "", fmt.Errorf("getting protocols for peer %s: %w", addrInfo.ID, err)
	}

	if len(x) == 0 {
		return "", fmt.Errorf("boost client cannot make a deal with storage provider %s because it does not support protocol version 1.2.0", maddr)
	}

	dealUuid := uuid.New()

	commp := dealP.Commp
	pieceCid, err := cid.Parse(commp)
	if err != nil {
		return "", fmt.Errorf("parsing commp '%s': %w", commp, err)
	}

	pieceSize := dealP.PieceSize
	if pieceSize == 0 {
		return "", fmt.Errorf("must provide piece-size parameter for CAR url")
	}

	payloadCidStr := dealP.PayloadCid
	rootCid, err := cid.Parse(payloadCidStr)
	if err != nil {
		return "", fmt.Errorf("dealUuid: %s, parsing payload cid %s: %w", dealUuid.String(), payloadCidStr, err)
	}

	carFileSize := dealP.CarSize
	if dealP.CarSize == 0 {
		return "", fmt.Errorf("size of car file cannot be 0")
	}

	transfer := types.Transfer{
		Size: carFileSize,
	}

	var providerCollateral abi.TokenAmount
	if dealP.ProviderCollateral != 0 {
		providerCollateral = abi.NewTokenAmount(int64(dealP.ProviderCollateral))
	} else {
		bounds, err := fullNode.StateDealProviderCollateralBounds(ctx, abi.PaddedPieceSize(pieceSize), dealP.Verified, chaintypes.EmptyTSK)
		if err != nil {
			return "", fmt.Errorf("dealUuid: %s, node error getting collateral bounds: %w", dealUuid.String(), err)
		}

		providerCollateral = big.Div(big.Mul(bounds.Min, big.NewInt(6)), big.NewInt(5)) // add 20%
	}

	tipset, err := fullNode.ChainHead(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot get chain head: %w", err)
	}

	head := tipset.Height()
	logs.GetLogger().Debug("current block height", "number", head)

	if dealP.StartEpoch != 0 && dealP.StartEpochHeadOffset != 0 {
		return "", errors.New("only one flag from `start-epoch-head-offset' or `start-epoch` can be specified")
	}

	var startEpoch abi.ChainEpoch

	if dealP.StartEpochHeadOffset != 0 {
		startEpoch = head + abi.ChainEpoch(dealP.StartEpochHeadOffset)
	} else if dealP.StartEpoch != 0 {
		startEpoch = abi.ChainEpoch(dealP.StartEpoch)
	} else {
		// default
		startEpoch = head + abi.ChainEpoch(5760) // head + 2 days
	}

	// Create a deal proposal to storage provider using deal protocol v1.2.0 format
	dealProposal, err := dealProposal(ctx, n, walletAddr, rootCid, abi.PaddedPieceSize(pieceSize), pieceCid, maddr, startEpoch, dealP.Duration, dealP.Verified, providerCollateral, abi.NewTokenAmount(int64(dealP.StoragePrice)))
	if err != nil {
		return "", fmt.Errorf("dealUuid: %s, failed to create a deal proposal: %w", dealUuid.String(), err)
	}

	dealParams := types.DealParams{
		DealUUID:           dealUuid,
		ClientDealProposal: *dealProposal,
		DealDataRoot:       rootCid,
		IsOffline:          true,
		Transfer:           transfer,
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	logs.GetLogger().Debug("about to submit deal proposal", "uuid", dealUuid.String())

	s, err := n.Host.NewStream(ctx, addrInfo.ID, DealProtocolv120)
	if err != nil {
		return "", fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
	}
	defer s.Close()

	var resp types.DealResponse
	if err := doRpc(ctx, s, &dealParams, &resp); err != nil {
		return "", fmt.Errorf("send proposal rpc: %w", err)
	}

	if !resp.Accepted {
		return "", fmt.Errorf("deal proposal rejected: %s", resp.Message)
	}

	fmt.Println("dealUuid: ", dealUuid.String(), ", the deal proposal has been sent to the storage provider, the deal info is as follows: ")
	out := map[string]interface{}{
		"dealUuid":           dealUuid.String(),
		"provider":           maddr.String(),
		"clientWallet":       walletAddr.String(),
		"payloadCid":         rootCid.String(),
		"commp":              dealProposal.Proposal.PieceCID.String(),
		"startEpoch":         dealProposal.Proposal.StartEpoch.String(),
		"endEpoch":           dealProposal.Proposal.EndEpoch.String(),
		"providerCollateral": dealProposal.Proposal.ProviderCollateral.String(),
	}
	return dealUuid.String(), cmd.PrintJson(out)
}

func dealProposal(ctx context.Context, n *clinode.Node, clientAddr address.Address, rootCid cid.Cid, pieceSize abi.PaddedPieceSize, pieceCid cid.Cid, minerAddr address.Address, startEpoch abi.ChainEpoch, duration int, verified bool, providerCollateral abi.TokenAmount, storagePrice abi.TokenAmount) (*market.ClientDealProposal, error) {
	endEpoch := startEpoch + abi.ChainEpoch(duration)
	// deal proposal expects total storage price for deal per epoch, therefore we
	// multiply pieceSize * storagePrice (which is set per epoch per GiB) and divide by 2^30
	storagePricePerEpochForDeal := big.Div(big.Mul(big.NewInt(int64(pieceSize)), storagePrice), big.NewInt(int64(1<<30)))
	l, err := market.NewLabelFromString(rootCid.String())
	if err != nil {
		return nil, err
	}
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         verified,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                l,
		StartEpoch:           startEpoch,
		EndEpoch:             endEpoch,
		StoragePricePerEpoch: storagePricePerEpochForDeal,
		ProviderCollateral:   providerCollateral,
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

	sig, err := n.Wallet.WalletSign(ctx, clientAddr, buf, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, fmt.Errorf("wallet sign failed: %w", err)
	}

	return &market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: *sig,
	}, nil
}

func doRpc(ctx context.Context, s inet.Stream, req interface{}, resp interface{}) error {
	errc := make(chan error)
	go func() {
		if err := cborutil.WriteCborRPC(s, req); err != nil {
			errc <- fmt.Errorf("failed to send request: %w", err)
			return
		}

		if err := cborutil.ReadCborRPC(s, resp); err != nil {
			errc <- fmt.Errorf("failed to read response: %w", err)
			return
		}

		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type DealParam struct {
	Provider             string `json:"provider"`                // storage provider on-chain address. Required
	Commp                string `json:"commp"`                   // commp of the CAR file. Required
	PieceSize            uint64 `json:"piece_size"`              // size of the CAR file as a padded piece. Required
	CarSize              uint64 `json:"car_size"`                // size of the CAR file. Required
	PayloadCid           string `json:"payload_cid"`             // root CID of the CAR file. Required
	StartEpoch           int    `json:"start_epoch"`             // start epoch by when the deal should be proved by provider on-chain. default: current chain head + 2 days
	StartEpochHeadOffset int    `json:"start_epoch_head_offset"` // start epoch head offset
	Duration             int    `json:"duration"`                // duration of the deal in epochs. default is 2880 * 180 == 180 days  518400
	ProviderCollateral   int    `json:"provider_collateral"`     // deal collateral that storage miner must put in escrow; if empty, the min collateral for the given piece size will be used
	StoragePrice         int    `json:"storage_price"`           // storage price in attoFIL per epoch per GiB. default 1
	Verified             bool   `json:"verified"`                // whether the deal funds should come from verified client data-cap. default true
	FastRetrieval        bool   `json:"fast_retrieval"`          // indicates that data should be available for fast retrieval. default true
	Wallet               string `json:"wallet"`                  // wallet address to be used to initiate the deal
}

func (client *Client) StorageAsk(provider string, size int64, duration int64) (*AskInfo, error) {
	ctx := context.Background()
	n, err := clinode.Setup(client.ClientRepo)
	if err != nil {
		return nil, err
	}
	defer n.Host.Close()

	ainfo := cliutil.ParseApiInfo(client.FullNodeApi)
	addr, err := ainfo.DialArgs("v1")
	if err != nil {
		logs.GetLogger().Error("parse fullNodeApi failed: %w", err)
		return nil, err
	}

	fullNode, closer, err := apiclient.NewFullNodeRPCV1(ctx, addr, ainfo.AuthHeader())
	if err != nil {
		return nil, fmt.Errorf("cant setup fullnode connection: %w", err)
	}
	defer closer()
	maddr, err := address.NewFromString(provider)
	if err != nil {
		return nil, err
	}

	addrInfo, err := cmd.GetAddrInfo(ctx, fullNode, maddr)
	if err != nil {
		return nil, err
	}
	logs.GetLogger().Debug("found storage provider", "id", addrInfo.ID, "multiaddrs", addrInfo.Addrs, "addr", maddr)

	if err := n.Host.Connect(ctx, *addrInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
	}

	s, err := n.Host.NewStream(ctx, addrInfo.ID, AskProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
	}
	defer s.Close()

	var resp network.AskResponse

	askRequest := network.AskRequest{
		Miner: maddr,
	}

	if err := doRpc(ctx, s, &askRequest, &resp); err != nil {
		return nil, fmt.Errorf("send ask request rpc: %w", err)
	}

	ask := resp.Ask.Ask

	logs.GetLogger().Infof("Ask: %s\n", maddr)
	logs.GetLogger().Infof("Price per GiB: %s\n", chaintypes.FIL(ask.Price))
	logs.GetLogger().Infof("Verified Price per GiB: %s\n", chaintypes.FIL(ask.VerifiedPrice))
	logs.GetLogger().Infof("Max Piece size: %s\n", chaintypes.SizeStr(chaintypes.NewInt(uint64(ask.MaxPieceSize))))
	logs.GetLogger().Infof("Min Piece size: %s\n", chaintypes.SizeStr(chaintypes.NewInt(uint64(ask.MinPieceSize))))
	info := &AskInfo{
		StorageAsk: *ask,
	}
	if size == 0 {
		return info, nil
	}
	perEpoch := chaintypes.BigDiv(chaintypes.BigMul(ask.Price, chaintypes.NewInt(uint64(size))), chaintypes.NewInt(1<<30))
	logs.GetLogger().Infof("Price per Block: %s\n", chaintypes.FIL(perEpoch))
	info.EpochPrice = perEpoch

	if duration == 0 {
		return info, nil
	}
	info.TotalPrice = chaintypes.BigMul(perEpoch, chaintypes.NewInt(uint64(duration)))
	logs.GetLogger().Infof("Total Price: %s\n", chaintypes.FIL(info.TotalPrice))

	return info, nil
}

type AskInfo struct {
	storagemarket.StorageAsk
	EpochPrice big.Int
	TotalPrice big.Int
}

func CheckDealConfig(lotusClient *lotus.LotusClient, dealConfig *model.DealConfig, lotusFirst ...bool) (pieceSize int64, epochPrice mbig.Int, err error) {
	first, last := CheckDealConfigByBoost, CheckDealConfigByLotus
	if len(lotusFirst) > 0 {
		first, last = CheckDealConfigByLotus, CheckDealConfigByBoost
	}
	pieceSize, epochPrice, err = first(lotusClient, dealConfig)
	if err == nil {
		return
	}
	return last(lotusClient, dealConfig)
}

func CheckDealConfigByLotus(lotusClient *lotus.LotusClient, dealConfig *model.DealConfig) (pieceSize int64, epochPrice mbig.Int, err error) {
	minerPrice, err := lotusClient.CheckDealConfig(dealConfig)
	if err != nil {
		logs.GetLogger().Error(err)
		return
	}
	pieceSize, sectorSize := utils.CalculatePieceSize(dealConfig.FileSize, false)
	cost := utils.CalculateRealCost(sectorSize, *minerPrice)
	epochPrice = *cost.Mul(decimal.NewFromFloat(constants.LOTUS_PRICE_MULTIPLE_1E18)).BigInt()
	return
}

func CheckDealConfigByBoost(lotusClient *lotus.LotusClient, dealConfig *model.DealConfig) (pieceSize int64, epochPrice mbig.Int, err error) {
	pieceSize, sectorSize := utils.CalculatePieceSize(dealConfig.FileSize, true)
	ask, err := GetClient(dealConfig.ClientRepo).WithClient(lotusClient).StorageAsk(dealConfig.MinerFid, int64(sectorSize), int64(dealConfig.Duration))
	if err != nil {
		logs.GetLogger().Error(err)
		return
	}
	epochPrice = *ask.EpochPrice.Int
	return
}

func ValidateDealConfig(lotusClient *lotus.LotusClient, dealConfig *model.DealConfig, boostFirst ...bool) (minerPrice *decimal.Decimal, isBoost bool, err error) {
	if dealConfig == nil {
		err = fmt.Errorf("parameter dealConfig is nil")
		logs.GetLogger().Error(err)
		return
	}

	if dealConfig.SenderWallet == "" {
		err = fmt.Errorf("wallet should be set")
		logs.GetLogger().Error(err)
		return
	}

	// query ask miner config
	var first, last QueryAsk
	if len(boostFirst) > 0 && boostFirst[0] {
		first, last = GetClient(dealConfig.ClientRepo).WithClient(lotusClient).QueryAsk, lotusClient.LotusClientQueryAsk
		isBoost = true
	} else {
		first, last = lotusClient.LotusClientQueryAsk, GetClient(dealConfig.ClientRepo).WithClient(lotusClient).QueryAsk
	}
	minerConfig, err := first(dealConfig.MinerFid)
	if err != nil {
		logs.GetLogger().Error(err)
		isBoost = !isBoost // note: this
		minerConfig, err = last(dealConfig.MinerFid)
		if err != nil {
			logs.GetLogger().Error(err)
			return
		}
	}

	// check deal with miner config
	minerPrice, err = CheckDealWithMinerConfig(lotusClient, dealConfig, minerConfig)
	return
}

type QueryAsk func(miner string) (*lotus.MinerConfig, error)

func (client *Client) QueryAsk(miner string) (*lotus.MinerConfig, error) {
	info, err := client.StorageAsk(miner, 0, 0)
	if err != nil {
		return nil, err
	}
	return &lotus.MinerConfig{
		Price:         decimal.NewFromBigInt(info.Price.Int, 0),
		VerifiedPrice: decimal.NewFromBigInt(info.VerifiedPrice.Int, 0),
		MinPieceSize:  int64(info.MinPieceSize),
		MaxPieceSize:  int64(info.MaxPieceSize),
	}, nil
}

func CheckDealWithMinerConfig(lotusClient *lotus.LotusClient, dealConfig *model.DealConfig, minerConfig *lotus.MinerConfig) (*decimal.Decimal, error) {
	if dealConfig.FileSize < minerConfig.MinPieceSize || dealConfig.FileSize > minerConfig.MaxPieceSize {
		err := fmt.Errorf("payload cid:%s, file size:%d is outside of miner:%s's range:[%d,%d]", dealConfig.PayloadCid, dealConfig.FileSize, dealConfig.MinerFid, minerConfig.MinPieceSize, minerConfig.MaxPieceSize)
		logs.GetLogger().Error(err)
		return nil, err
	}

	e18 := decimal.NewFromFloat(constants.LOTUS_PRICE_MULTIPLE_1E18)
	var minerPrice decimal.Decimal
	if dealConfig.VerifiedDeal {
		minerPrice = minerConfig.VerifiedPrice.Div(e18)
	} else {
		minerPrice = minerConfig.Price.Div(e18)
	}
	logs.GetLogger().Info("miner: ", dealConfig.MinerFid, ", price: ", minerPrice)

	priceCmp := dealConfig.MaxPrice.Cmp(minerPrice)
	if priceCmp < 0 {
		err := fmt.Errorf("miner price:%s > deal max price:%s", minerPrice.String(), dealConfig.MaxPrice.String())
		logs.GetLogger().Error(err)
		return nil, err
	}

	if dealConfig.Duration == 0 {
		dealConfig.Duration = constants.DURATION_DEFAULT
	}
	if err := lotusClient.CheckDuration(dealConfig.Duration, dealConfig.StartEpoch); err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	return &minerPrice, nil
}
