type RetryableImportedModelSource = {
  toJSON: () => Record<string, unknown>
}

export function buildRetryableImportedModelFactory<T> (
  model: RetryableImportedModelSource,
  instantiate: (attributes: Record<string, unknown>) => T
) {
  const attributes = buildRetryableImportedModelAttributes(model)

  return () => instantiate({ ...attributes })
}

export function buildRetryableImportedModelAttributes (model: RetryableImportedModelSource) {
  const { id, createdAt, updatedAt, ...attributes } = model.toJSON()
  void id
  void createdAt
  void updatedAt

  return attributes
}
