import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const root = process.cwd();
const read = (path: string) => readFileSync(resolve(root, path), "utf8");

describe("independent composition packaging", () => {
  it("builds composition only from its new owning folder", () => {
    const compose = read("../../docker-compose.yml");
    const start = compose.indexOf("  recording-composition:");
    const end = compose.indexOf("\n  postgres:", start);
    const service = compose.slice(start, end);
    expect(service).toContain("context: ./recording-bot");
    expect(service).toContain('command: ["node", "dist/composition.js"]');
    const workerCompose = read("../docker-compose.yml");
    expect(workerCompose).toContain("image: ${RECORDING_IMAGE:-conference-recording-bot:local}");
    expect(workerCompose).toContain('command: ["node", "dist/composition.js"]');
  });

  it("keeps Node dependencies outside the parent Go module traversal", () => {
    expect(read("go.mod")).toContain("module conference/recording-bot/composition-tooling");
    expect(read(".dockerignore")).toContain("go.mod");
    expect(read("../.dockerignore")).toContain("composition/");
    const ignored = read(".dockerignore");
    for (const artifact of ["node_modules", "dist", ".npm-cache", "coverage", ".vitest"]) {
      expect(ignored).toContain(artifact);
    }
  });

});
