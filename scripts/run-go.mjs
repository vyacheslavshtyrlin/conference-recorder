import { spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("run-go requires a Go command");
  process.exit(2);
}

const goEnvironment = { ...process.env };
if (process.platform === "win32") {
  const cacheDirectory = resolve(".gocache");
  const temporaryDirectory = resolve("tmp");
  mkdirSync(cacheDirectory, { recursive: true });
  mkdirSync(temporaryDirectory, { recursive: true });
  goEnvironment.GOCACHE = cacheDirectory;
  goEnvironment.TEMP = temporaryDirectory;
  goEnvironment.TMP = temporaryDirectory;
}

const local = spawnSync("go", args, { stdio: "inherit", env: goEnvironment });
if (!local.error) process.exit(local.status ?? 1);
if (local.error.code !== "ENOENT") throw local.error;

console.warn("Go is not installed on this runner; using the pipeline's Docker daemon.");
const mountedSource = `${process.cwd()}:/src`;
const fallback = spawnSync("docker", [
  "run",
  "--rm",
  "--volume",
  mountedSource,
  "--workdir",
  "/src",
  "--env",
  "GOCACHE=/tmp/go-build-cache",
  "golang:1.24-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191",
  "go",
  ...args,
], { stdio: "inherit" });

if (fallback.error) throw fallback.error;
process.exit(fallback.status ?? 1);
