import type { Participant, Producer } from "./contracts.js";

const WIDTH = 1280;
const HEIGHT = 720;
const PADDING = 28;
const GAP = 18;

type Rect = { x: number; y: number; width: number; height: number };

export class CanvasComposer {
  private readonly context: CanvasRenderingContext2D;

  constructor(
    private readonly canvas: HTMLCanvasElement,
    private readonly participants: ReadonlyMap<string, Participant>,
    private readonly producers: ReadonlyMap<string, Producer>,
    private readonly videos: ReadonlyMap<string, HTMLVideoElement>,
  ) {
    this.context = canvas.getContext("2d", { alpha: false })!;
  }

  captureStream(frameRate: number): MediaStream {
    return this.canvas.captureStream(frameRate);
  }

  drawFrame(): void {
    this.drawBackground();
    const screen = [...this.producers.values()].find(
      (producer) =>
        producer.source === "screen" &&
        !producer.paused &&
        this.videos.has(producer.producerId),
    );
    if (screen) {
      this.drawScreenShare(screen);
      return;
    }
    this.drawGallery([...this.participants.values()]);
  }

  private drawScreenShare(screen: Producer): void {
    const thumbnails = [...this.participants.values()].filter(
      (participant) => participant.participantId !== screen.participantId,
    );
    const layout = screenLayout(thumbnails.length);
    this.drawScreenStage(screen, layout.stage);
    thumbnails.slice(0, layout.tiles.length).forEach((participant, index) =>
      this.drawParticipantTile(participant, layout.tiles[index]!),
    );
    if (thumbnails.length > layout.tiles.length)
      this.drawOverflowTile(
        thumbnails.length - layout.tiles.length + 1,
        layout.tiles.at(-1)!,
      );
  }

  private drawGallery(participants: Participant[]): void {
    if (participants.length === 0) return;
    const columns = Math.ceil(Math.sqrt((participants.length * 16) / 9));
    const rows = Math.ceil(participants.length / columns);
    const width = (WIDTH - PADDING * 2 - GAP * (columns - 1)) / columns;
    const height = (HEIGHT - PADDING * 2 - GAP * (rows - 1)) / rows;
    participants.forEach((participant, index) =>
      this.drawParticipantTile(participant, {
        x: PADDING + (index % columns) * (width + GAP),
        y: PADDING + Math.floor(index / columns) * (height + GAP),
        width,
        height,
      }),
    );
  }

  private drawScreenStage(screen: Producer, rect: Rect): void {
    const ctx = this.context;
    this.drawCard(rect, 20, "#080d16");
    const content = {
      x: rect.x + 12,
      y: rect.y + 50,
      width: rect.width - 24,
      height: rect.height - 62,
    };
    this.drawVideo(screen, content, true, 12);

    ctx.fillStyle = "rgba(6, 12, 22, .94)";
    roundRect(ctx, rect.x + 12, rect.y + 12, rect.width - 24, 28, 10);
    ctx.fill();
    ctx.fillStyle = "#7dd3fc";
    ctx.beginPath();
    ctx.arc(rect.x + 30, rect.y + 26, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = "#f8fafc";
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ctx.font = "600 17px sans-serif";
    ctx.fillText(
      `Screen share · ${truncate(screen.displayName, 72)}`,
      rect.x + 44,
      rect.y + 26,
      rect.width - 68,
    );
  }

  private drawParticipantTile(participant: Participant, rect: Rect): void {
    this.drawCard(rect, 16, "#172033");
    const camera = [...this.producers.values()].find(
      (producer) =>
        producer.participantId === participant.participantId &&
        producer.source === "camera" &&
        !producer.paused &&
        this.videos.has(producer.producerId),
    );
    if (camera) this.drawVideo(camera, rect, false, 16);
    else this.drawAvatar(participant.displayName, rect, 16);
    this.drawLabel(participant.displayName, rect);
  }

  private drawVideo(
    producer: Producer,
    rect: Rect,
    contain: boolean,
    radius: number,
  ): void {
    const video = this.videos.get(producer.producerId);
    if (!video || video.videoWidth === 0 || video.videoHeight === 0) {
      this.drawAvatar(producer.displayName, rect, radius);
      return;
    }
    const scale = contain
      ? Math.min(rect.width / video.videoWidth, rect.height / video.videoHeight)
      : Math.max(rect.width / video.videoWidth, rect.height / video.videoHeight);
    const drawWidth = video.videoWidth * scale;
    const drawHeight = video.videoHeight * scale;
    const ctx = this.context;
    ctx.save();
    roundRect(ctx, rect.x, rect.y, rect.width, rect.height, radius);
    ctx.clip();
    ctx.drawImage(
      video,
      rect.x + (rect.width - drawWidth) / 2,
      rect.y + (rect.height - drawHeight) / 2,
      drawWidth,
      drawHeight,
    );
    ctx.restore();
  }

  private drawAvatar(name: string, rect: Rect, radius: number): void {
    const ctx = this.context;
    const gradient = ctx.createLinearGradient(
      rect.x,
      rect.y,
      rect.x + rect.width,
      rect.y + rect.height,
    );
    gradient.addColorStop(0, "#243b66");
    gradient.addColorStop(1, "#141d31");
    ctx.fillStyle = gradient;
    roundRect(ctx, rect.x, rect.y, rect.width, rect.height, radius);
    ctx.fill();
    ctx.fillStyle = "#8fb4f8";
    ctx.beginPath();
    ctx.arc(
      rect.x + rect.width / 2,
      rect.y + rect.height / 2,
      Math.min(rect.width, rect.height) * 0.2,
      0,
      Math.PI * 2,
    );
    ctx.fill();
    ctx.fillStyle = "#07101f";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.font = `${Math.max(18, Math.min(rect.width, rect.height) * 0.18)}px sans-serif`;
    ctx.fillText(initials(name), rect.x + rect.width / 2, rect.y + rect.height / 2);
  }

  private drawLabel(name: string, rect: Rect): void {
    const ctx = this.context;
    const label = truncate(name, 40);
    const fontSize = Math.max(14, Math.min(18, rect.width * 0.09));
    ctx.font = `600 ${fontSize}px sans-serif`;
    const labelWidth = Math.min(
      rect.width - 20,
      ctx.measureText(label).width + 24,
    );
    ctx.fillStyle = "rgba(3, 8, 18, .82)";
    roundRect(ctx, rect.x + 10, rect.y + rect.height - 42, labelWidth, 30, 9);
    ctx.fill();
    ctx.fillStyle = "#f8fafc";
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ctx.fillText(label, rect.x + 22, rect.y + rect.height - 27, labelWidth - 16);
  }

  private drawOverflowTile(count: number, rect: Rect): void {
    this.drawCard(rect, 16, "#1e293b");
    const ctx = this.context;
    ctx.fillStyle = "#f8fafc";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.font = "600 28px sans-serif";
    ctx.fillText(`+${count}`, rect.x + rect.width / 2, rect.y + rect.height / 2);
  }

  private drawCard(rect: Rect, radius: number, color: string): void {
    const ctx = this.context;
    ctx.fillStyle = color;
    roundRect(ctx, rect.x, rect.y, rect.width, rect.height, radius);
    ctx.fill();
  }

  private drawBackground(): void {
    const ctx = this.context;
    const gradient = ctx.createLinearGradient(0, 0, WIDTH, HEIGHT);
    gradient.addColorStop(0, "#17223a");
    gradient.addColorStop(0.46, "#090f1c");
    gradient.addColorStop(1, "#131b2c");
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
    const glow = ctx.createRadialGradient(WIDTH * 0.72, 0, 0, WIDTH * 0.72, 0, HEIGHT);
    glow.addColorStop(0, "rgba(56, 189, 248, .14)");
    glow.addColorStop(1, "rgba(56, 189, 248, 0)");
    ctx.fillStyle = glow;
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
    ctx.strokeStyle = "rgba(148, 163, 184, .055)";
    ctx.lineWidth = 1;
    for (let x = 0; x <= WIDTH; x += 64) {
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, HEIGHT);
      ctx.stroke();
    }
    for (let y = 0; y <= HEIGHT; y += 64) {
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(WIDTH, y);
      ctx.stroke();
    }
  }
}

function screenLayout(count: number): { stage: Rect; tiles: Rect[] } {
  if (count <= 2) {
    const tile = 244;
    const railX = WIDTH - PADDING - tile;
    const stage = {
      x: PADDING,
      y: PADDING,
      width: railX - GAP - PADDING,
      height: HEIGHT - PADDING * 2,
    };
    const startY = (HEIGHT - (count * tile + Math.max(0, count - 1) * GAP)) / 2;
    return {
      stage,
      tiles: Array.from({ length: count }, (_, index) => ({
        x: railX,
        y: startY + index * (tile + GAP),
        width: tile,
        height: tile,
      })),
    };
  }

  if (count <= 4) {
    const tile = 150;
    const stage = { x: 206, y: PADDING, width: 868, height: HEIGHT - PADDING * 2 };
    const leftX = PADDING;
    const rightX = WIDTH - PADDING - tile;
    const topY = PADDING + 105;
    const bottomY = HEIGHT - PADDING - 105 - tile;
    return {
      stage,
      tiles: [
        { x: leftX, y: topY, width: tile, height: tile },
        { x: rightX, y: topY, width: tile, height: tile },
        { x: leftX, y: bottomY, width: tile, height: tile },
        { x: rightX, y: bottomY, width: tile, height: tile },
      ].slice(0, count),
    };
  }

  const tile = 136;
  const visible = Math.min(count, 8);
  const totalWidth = visible * tile + (visible - 1) * GAP;
  const startX = Math.max(PADDING, (WIDTH - totalWidth) / 2);
  return {
    stage: { x: PADDING, y: PADDING, width: WIDTH - PADDING * 2, height: 470 },
    tiles: Array.from({ length: visible }, (_, index) => ({
      x: startX + index * (tile + GAP),
      y: HEIGHT - PADDING - tile,
      width: tile,
      height: tile,
    })),
  };
}

function roundRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number,
): void {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + width, y, x + width, y + height, r);
  ctx.arcTo(x + width, y + height, x, y + height, r);
  ctx.arcTo(x, y + height, x, y, r);
  ctx.arcTo(x, y, x + width, y, r);
  ctx.closePath();
}

function truncate(value: string, maximum: number): string {
  return value.length > maximum ? `${value.slice(0, maximum - 1)}…` : value;
}

function initials(name: string): string {
  return (
    name
      .trim()
      .split(/\s+/)
      .slice(0, 2)
      .map((part) => part[0]?.toUpperCase() ?? "")
      .join("") || "?"
  );
}