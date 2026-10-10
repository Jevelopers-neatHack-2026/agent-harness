// Schema-5 generic checkpoint bounds. Keep these limits shared by the store,
// the private controller, and recovery's projection of the same checkpoint.
export const MAX_CHECKPOINT_DEPTH = 128;
export const MAX_CHECKPOINT_NODES = 10_000;
export const MAX_CHECKPOINT_SOURCE_UNITS = 256 * 1024;
export const MAX_CHECKPOINT_SERIALIZED_BYTES = 1024 * 1024;

// Recovery wraps the generic checkpoint in workflow, lease and attempt facts.
// This is fixed wrapper space, not an enlargement of the stored checkpoint.
export const RECOVERY_SNAPSHOT_WRAPPER_BYTES = 8 * 1024;
