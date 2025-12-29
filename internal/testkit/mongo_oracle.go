// Package testkit provides testing utilities for MonoLite.
package testkit

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OracleOptions configures the MongoDB Oracle.
type OracleOptions struct {
	// Image specifies the Docker image (default: "mongo:7.0")
	Image string
	// ReplSet enables replica set mode for transaction testing
	ReplSet bool
	// Timeout for container startup
	Timeout time.Duration
	// URI overrides automatic container creation (use existing MongoDB)
	URI string
}

// DefaultOracleOptions returns sensible defaults.
func DefaultOracleOptions() OracleOptions {
	return OracleOptions{
		Image:   "mongo:7.0",
		ReplSet: false,
		Timeout: 60 * time.Second,
		URI:     "",
	}
}

// MongoOracle represents a MongoDB instance used as a reference oracle.
type MongoOracle struct {
	URI    string
	Client *mongo.Client
	close  func()
}

// StartMongoOracle starts a MongoDB Oracle for differential testing.
// If MONOLITE_TEST_WITH_MONGO is not set, the test will be skipped.
// If MONOLITE_TEST_MONGO_URI is set, uses that URI instead of starting a container.
func StartMongoOracle(t *testing.T, opts OracleOptions) *MongoOracle {
	t.Helper()

	// Check if MongoDB testing is enabled
	if os.Getenv("MONOLITE_TEST_WITH_MONGO") == "" {
		t.Skip("MongoDB differential testing disabled (set MONOLITE_TEST_WITH_MONGO=1 to enable)")
	}

	// Check for existing MongoDB URI
	uri := os.Getenv("MONOLITE_TEST_MONGO_URI")
	if uri == "" && opts.URI != "" {
		uri = opts.URI
	}

	if uri == "" {
		// Start MongoDB container using testcontainers
		uri = startMongoContainer(t, opts)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("failed to connect to MongoDB oracle: %v", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		t.Fatalf("failed to ping MongoDB oracle: %v", err)
	}

	return &MongoOracle{
		URI:    uri,
		Client: client,
		close: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client.Disconnect(ctx)
		},
	}
}

// Close disconnects from the MongoDB Oracle.
func (o *MongoOracle) Close() {
	if o.close != nil {
		o.close()
	}
}

// Database returns a handle to the specified database.
func (o *MongoOracle) Database(name string) *mongo.Database {
	return o.Client.Database(name)
}

// startMongoContainer starts a MongoDB container using testcontainers-go.
// This is a stub that requires testcontainers-go dependency.
func startMongoContainer(t *testing.T, opts OracleOptions) string {
	t.Helper()

	// Check if testcontainers is available
	// For now, we'll provide a fallback to manual MongoDB setup
	t.Logf("Note: testcontainers-go not configured. Set MONOLITE_TEST_MONGO_URI to use an existing MongoDB instance.")

	// Try common local MongoDB addresses
	localURIs := []string{
		"mongodb://localhost:27017",
		"mongodb://127.0.0.1:27017",
	}

	for _, uri := range localURIs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err == nil {
			if err := client.Ping(ctx, nil); err == nil {
				client.Disconnect(ctx)
				cancel()
				t.Logf("Found local MongoDB at %s", uri)
				return uri
			}
			client.Disconnect(ctx)
		}
		cancel()
	}

	t.Skip("No MongoDB available. Either set MONOLITE_TEST_MONGO_URI or start a local MongoDB instance.")
	return ""
}

// RequireReplSet skips the test if replica set is not available.
func RequireReplSet(t *testing.T) {
	t.Helper()
	if os.Getenv("MONOLITE_TEST_MONGO_REPLSET") != "1" {
		t.Skip("Replica set required (set MONOLITE_TEST_MONGO_REPLSET=1)")
	}
}

// DiffClients holds both MonoLite and MongoDB clients for differential testing.
type DiffClients struct {
	MonoLite *MonoLiteHandle
	MongoDB  *MongoOracle
}

// StartDiffClients starts both MonoLite and MongoDB for differential testing.
func StartDiffClients(t *testing.T) *DiffClients {
	t.Helper()

	monoLite := StartMonoLite(t)
	mongoOracle := StartMongoOracle(t, DefaultOracleOptions())

	return &DiffClients{
		MonoLite: monoLite,
		MongoDB:  mongoOracle,
	}
}

// Close closes both clients.
func (dc *DiffClients) Close() {
	if dc.MonoLite != nil {
		dc.MonoLite.Close()
	}
	if dc.MongoDB != nil {
		dc.MongoDB.Close()
	}
}

