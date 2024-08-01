package myask

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/filecoin-project/boost/markets/shared"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
	"os"
)

var log = logging.Logger("storedask")

// DefaultPrice is the default price for unverified deals (in attoFil / GiB / Epoch)
var DefaultPrice = abi.NewTokenAmount(50000000)

// DefaultVerifiedPrice is the default price for verified deals (in attoFil / GiB / Epoch)
var DefaultVerifiedPrice = abi.NewTokenAmount(5000000)

// DefaultDuration is the default number of epochs a storage ask is in effect for
const DefaultDuration abi.ChainEpoch = 1000000

// DefaultMinPieceSize is the minimum accepted piece size for data
const DefaultMinPieceSize abi.PaddedPieceSize = 256

// DefaultMaxPieceSize is the default maximum accepted size for pieces for deals
// TODO: It would be nice to default this to the miner's sector size
const DefaultMaxPieceSize abi.PaddedPieceSize = 32 << 30

type StoredAsk interface {
	GetAsk(miner address.Address) *legacytypes.SignedStorageAsk
	SetAsk(ctx context.Context, price abi.TokenAmount, verifiedPrice abi.TokenAmount, duration abi.ChainEpoch, miner address.Address, options ...legacytypes.StorageAskOption) error
}

type storedAsk struct {
	asks     map[address.Address]*legacytypes.SignedStorageAsk
	fullNode api.FullNode
	db       *StorageAskDB
}

// NewStoredAsk returns a new instance of StoredAsk
// It will initialize a new SignedStorageAsk on disk if one is not set
// Otherwise it loads the current SignedStorageAsk from disk
func NewStoredAsk(repo string, fullNode api.FullNode) (*storedAsk, error) {
	path, err := homedir.Expand(repo)
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("boost-repo at '%s' is not initialized", repo)
	}

	askDb, err := NewStorageAskDB(repo)
	err = createAskTable(context.TODO(), askDb.db)
	if err != nil {
		return nil, err
	}

	s := &storedAsk{
		fullNode: fullNode,
		db:       askDb,
		asks:     make(map[address.Address]*legacytypes.SignedStorageAsk),
	}

	return s, nil
}

func signBytes(ctx context.Context, signer address.Address, b []byte, f api.FullNode) (*crypto.Signature, error) {
	signer, err := f.StateAccountKey(ctx, signer, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	log.Debugf("signing the ask %s with address %s", string(b), signer.String())

	localSignature, err := f.WalletSign(ctx, signer, b)
	if err != nil {
		return nil, err
	}
	return localSignature, nil
}

func getMinerWorkerAddress(ctx context.Context, maddr address.Address, tok shared.TipSetToken, f api.FullNode) (address.Address, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return address.Undef, err
	}

	mi, err := f.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return address.Address{}, err
	}
	return mi.Worker, nil
}

func (s *storedAsk) sign(ctx context.Context, ask *legacytypes.StorageAsk) (*crypto.Signature, error) {
	tok, err := s.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	return signMinerData(ctx, ask, ask.Miner, tok.Key().Bytes(), s.fullNode)
}

// SignMinerData signs the given data structure with a signature for the given address
func signMinerData(ctx context.Context, data interface{}, address address.Address, tok shared.TipSetToken, f api.FullNode) (*crypto.Signature, error) {
	msg, err := cborutil.Dump(data)
	if err != nil {
		return nil, xerrors.Errorf("serializing: %w", err)
	}

	worker, err := getMinerWorkerAddress(ctx, address, tok, f)
	if err != nil {
		return nil, err
	}

	sig, err := signBytes(ctx, worker, msg, f)
	if err != nil {
		return nil, xerrors.Errorf("failed to sign: %w", err)
	}
	return sig, nil
}

func (s *storedAsk) SetAsk(ctx context.Context, price abi.TokenAmount, verifiedPrice abi.TokenAmount, duration abi.ChainEpoch, miner address.Address, options ...legacytypes.StorageAskOption) error {
	minerAsk, err := s.getSignedAsk(ctx, miner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("get miner ask data failed, error: %w", err)
	}
	s.asks[miner] = &minerAsk

	var seqno uint64
	minPieceSize := DefaultMinPieceSize
	maxPieceSize := DefaultMaxPieceSize

	oldAsk, ok := s.asks[miner]
	if ok && oldAsk.Ask != nil {
		seqno = oldAsk.Ask.SeqNo + 1
		minPieceSize = oldAsk.Ask.MinPieceSize
		maxPieceSize = oldAsk.Ask.MaxPieceSize
	}

	ts, err := s.fullNode.ChainHead(ctx)
	if err != nil {
		return err
	}
	ask := &legacytypes.StorageAsk{
		Price:         price,
		VerifiedPrice: verifiedPrice,
		Timestamp:     ts.Height(),
		Expiry:        ts.Height() + duration,
		Miner:         miner,
		SeqNo:         seqno,
		MinPieceSize:  minPieceSize,
		MaxPieceSize:  maxPieceSize,
	}

	for _, option := range options {
		option(ask)
	}

	sig, err := s.sign(ctx, ask)
	if err != nil {
		return err
	}

	s.asks[miner] = &legacytypes.SignedStorageAsk{
		Ask:       ask,
		Signature: sig,
	}
	return s.storeAsk(ctx, *ask)

}

func (s *storedAsk) getSignedAsk(ctx context.Context, miner address.Address) (legacytypes.SignedStorageAsk, error) {
	ask, err := s.db.Get(ctx, miner)
	if err != nil {
		return legacytypes.SignedStorageAsk{}, err
	}
	ss, err := s.sign(ctx, &ask)
	if err != nil {
		return legacytypes.SignedStorageAsk{}, nil
	}

	return legacytypes.SignedStorageAsk{
		Ask:       &ask,
		Signature: ss,
	}, nil
}

func (s *storedAsk) storeAsk(ctx context.Context, ask legacytypes.StorageAsk) error {
	return s.db.Update(ctx, ask)
}
