import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const root = process.cwd();
const read = (path: string) => readFileSync(resolve(root, path), "utf8");

describe("container-safe composition packaging", () => {
  it("contains no Chromium capture runtime dependency or production import", () => {
    const packageJson = read("package.json");
    const production = ["composition.ts", "compositionV2.ts", "compositionV2Executor.ts", "config.ts", "processRunner.ts"]
      .map((name) => read(`src/${name}`)).join("\n");
    for (const forbidden of ["playwright", "chromium", "mediasoup", "recordingsink"]) {
      expect(packageJson.toLowerCase()).not.toContain(forbidden);
      expect(production.toLowerCase()).not.toContain(forbidden);
    }
    expect(production).not.toContain("@ffmpeg-installer");
    expect(production).not.toContain("@ffprobe-installer");
  });

  it("packages pinned FFmpeg, FFprobe, fonts and a real readiness probe", () => {
    const dockerfile = read("Dockerfile");
    expect(dockerfile).toContain("ARG FFMPEG_PACKAGE_VERSION=6.1.2-r1");
    expect(dockerfile).toContain("ARG FONT_PACKAGE_VERSION=2.37-r5");
    expect(dockerfile).toContain("ARG SU_EXEC_PACKAGE_VERSION=0.2-r3");
    expect(dockerfile).toContain("ffmpeg -version");
    expect(dockerfile).toContain("ffprobe -version");
    expect(dockerfile).toContain('ENTRYPOINT ["recording-composition-entrypoint"]');
    expect(read("docker-entrypoint.sh")).toContain('exec su-exec node "$@"');
    expect(dockerfile).toContain("HEALTHCHECK");
    expect(dockerfile).toContain("http://127.0.0.1:8090/health/ready");
    expect(dockerfile).not.toMatch(/\bEXPOSE\b/);
  });
});
