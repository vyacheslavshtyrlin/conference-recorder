export class RevisionConflictError extends Error {}
export class RevisionGapError extends Error {}

export class RevisionTracker {
  private revision = -1;
  private readonly hashes = new Map<number, string>();

  reset(revision: number): void {
    this.revision = revision;
    this.hashes.clear();
  }

  accept(event: Record<string, unknown>): "apply" | "duplicate" {
    const revision = Number(event.revision);
    const hash = JSON.stringify(event);
    if (revision <= this.revision) {
      if (this.hashes.get(revision) === hash) return "duplicate";
      throw new RevisionConflictError("conflicting producer revision");
    }
    if (revision !== this.revision + 1)
      throw new RevisionGapError("producer revision gap");
    this.revision = revision;
    this.hashes.set(revision, hash);
    while (this.hashes.size > 256)
      this.hashes.delete(this.hashes.keys().next().value!);
    return "apply";
  }
}
