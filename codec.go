// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package codec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const (
	remoteCodecName = "temporal.io/remote-codec"
)

type EncodedPayload struct {
	Metadata  []byte `db:"metadata"`
	Data      []byte `db:"data"`
	Sha256Sum []byte `db:"sha256"`
}

type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

type Codec struct {
	setter func(ctx context.Context, db DBTX, payload EncodedPayload) (id int64, err error)
	getter func(ctx context.Context, db DBTX, id int64) (payload EncodedPayload, err error)
	pgpool *pgxpool.Pool
	// version is the LPS API version (v1 or v2).
	version string
	// minBytes is the minimum size of the payload in order to use remote codec.
	minBytes int
	// namespace is the Temporal namespace the client using this codec is connected to.
	namespace string
}

type remotePayload struct {
	// Content of the original payload's Metadata.
	Metadata map[string][]byte `json:"metadata"`
	// Number of bytes in the payload Data.
	Size uint `json:"size"`
	// Digest of the payload Data, prefixed with the algorithm, e.g. sha256:deadbeef.
	Digest string `json:"digest"`
	// The key to retrieve the payload from remote storage.
	Key string `json:"key"`
}

type Option interface {
	apply(*Codec) error
}

type applier func(*Codec) error

func (a applier) apply(c *Codec) error {
	return a(c)
}

// WithMinBytes configures the minimum size of an event payload needed to trigger
// encoding using the large payload codec. Any payload smaller than this value
// will be transparently persisted in workflow history.
//
// The default value is 128000, or 128KB.
//
// Setting this too low can lead to degraded performance, since decoding requires
// an additional network round trip per payload. This can add up quickly when
// replaying a workflow with a large number of events.
//
// According to https://docs.temporal.io/workflows, the hard limit for workflow
// history size is 50k events and 50MB. A workflow with exactly 50k events can
// therefore in theory have an average event payload size of 1048 bytes.
//
// In practice this worst case is very unlikely, since common workflow event
// types such as WorkflowTaskScheduled or WorkflowTaskCompleted do not include user
// defined payloads. If we estimate that one quarter of events have payloads just
// below the cutoff, then we can calculate how many events total would fit in
// one workflow's history (the point before which we must call ContinueAsNew):
//
//	AverageNonUserTaskBytes = 1024 (generous estimate for events like WorkflowTaskScheduled)
//	CodecMinBytes = 128_000
//	AverageEventBytes = (AverageNonUserTaskBytes * 3 + CodecMinBytes) / 4 = 32_768
//	MaxHistoryEventBytes = 50_000_000
//	MaxHistoryEventCount = MaxHistoryEventBytes / AverageEventBytes = 1525
func WithMinBytes(bytes uint32) Option {
	return applier(func(c *Codec) error {
		c.minBytes = int(bytes)
		return nil
	})
}

func WithPool(pool *pgxpool.Pool) Option {
	return applier(func(c *Codec) error {
		c.pgpool = pool
		return nil
	})
}

func WithGetter(g func(ctx context.Context, db DBTX, id int64) (payload EncodedPayload, err error)) Option {
	return applier(func(c *Codec) error {
		c.getter = g
		return nil
	})
}

func WithSetter(s func(ctx context.Context, db DBTX, payload EncodedPayload) (id int64, err error)) Option {
	return applier(func(c *Codec) error {
		c.setter = s
		return nil
	})
}

// WithNamespace sets the Temporal namespace the client using this codec is connected to.
// This option is mandatory.
func WithNamespace(namespace string) Option {
	return applier(func(c *Codec) error {
		c.namespace = namespace
		return nil
	})
}

// WithVersion sets the version of the LPS API to use.
func WithVersion(version string) Option {
	return applier(func(c *Codec) error {
		c.version = version
		return nil
	})
}

// New instantiates a Codec. WithURL is a required option.
//
// An error may be returned if incompatible options are configured or if a
// connection to the remote payload storage service cannot be established.
func New(opts ...Option) (*Codec, error) {
	c := Codec{
		// 128KB happens to be the lower bound for blobs eligible for AWS S3
		// Intelligent-Tiering:
		// https://aws.amazon.com/s3/storage-classes/intelligent-tiering/
		minBytes: 128_000,
	}

	for _, opt := range opts {
		if err := opt.apply(&c); err != nil {
			return nil, err
		}
	}

	if c.namespace == "" {
		return nil, fmt.Errorf("a namespace is required")
	}

	if c.version == "" {
		c.version = "v1"
	}

	if c.version != "v1" {
		return nil, fmt.Errorf("invalid codec version: %s", c.version)
	}

	// Check connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.pgpool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed connecting to pgpool: %w", err)
	}

	return &c, nil
}

func (c *Codec) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	var (
		ctx    = context.Background()
		result = make([]*common.Payload, len(payloads))
	)
	for i, payload := range payloads {
		norgle := c.minBytes
		poop := payload.Size()
		if poop > norgle {
			encodePayload, err := c.encodePayload(ctx, payload)
			if err != nil {
				return nil, err
			}
			result[i] = encodePayload
		} else {
			result[i] = payload
		}
	}

	return result, nil
}

func stringDigest(byteDigest []byte) string {
	return "sha256:" + hex.EncodeToString(byteDigest)
}

func (c *Codec) encodePayload(ctx context.Context, payload *common.Payload) (*common.Payload, error) {
	data := payload.GetData()
	sha2 := sha256.New()
	sha2.Write(payload.GetData())
	byteDigest := sha2.Sum(nil)

	// Set metadata header
	md, err := json.Marshal(payload.GetMetadata())
	if err != nil {
		return nil, err
	}

	id, err := c.setter(ctx, c.pgpool, EncodedPayload{
		Metadata:  md,
		Data:      data,
		Sha256Sum: byteDigest,
	})
	if err != nil {
		return nil, err
	}

	result, err := converter.GetDefaultDataConverter().ToPayload(remotePayload{
		Metadata: payload.GetMetadata(),
		Size:     uint(len(payload.GetData())),
		Digest:   stringDigest(byteDigest),
		Key:      fmt.Sprint(id),
	})
	if err != nil {
		return nil, err
	}
	result.Metadata[remoteCodecName] = []byte(c.version)

	return result, nil
}

func (c *Codec) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	result := make([]*common.Payload, len(payloads))
	for i, payload := range payloads {
		if codecVersion, ok := payload.GetMetadata()[remoteCodecName]; ok {
			switch string(codecVersion) {
			case "v1", "v2":
				decodedPayload, err := c.decodePayload(context.Background(), payload, string(codecVersion))
				if err != nil {
					return nil, err
				}
				result[i] = decodedPayload
			default:
				return nil, fmt.Errorf("unknown version for %s: %s", remoteCodecName, codecVersion)
			}
		} else {
			result[i] = payload
		}
	}
	return result, nil
}

func (c *Codec) decodePayload(ctx context.Context, payload *common.Payload, version string) (*common.Payload, error) {
	var remoteP remotePayload
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &remoteP); err != nil {
		return nil, err
	}

	// Parse remoteP.Key into an int64.
	key, err := strconv.ParseInt(remoteP.Key, 10, 64)
	if err != nil {
		return nil, err
	}
	encodedPayload, err := c.getter(ctx, c.pgpool, key)
	if err != nil {
		return nil, err
	}

	sha2 := sha256.New()
	_, err = sha2.Write(encodedPayload.Data)
	if err != nil {
		return nil, err
	}

	if uint(len(encodedPayload.Data)) != remoteP.Size {
		return nil, fmt.Errorf("wanted object of size %d, got %d", remoteP.Size, len(encodedPayload.Data))
	}

	checkSum := stringDigest(sha2.Sum(nil))

	if checkSum != remoteP.Digest {
		return nil, fmt.Errorf("wanted object sha %s, got %s", remoteP.Digest, checkSum)
	}

	return &common.Payload{
		Metadata: remoteP.Metadata,
		Data:     encodedPayload.Data,
	}, nil
}
