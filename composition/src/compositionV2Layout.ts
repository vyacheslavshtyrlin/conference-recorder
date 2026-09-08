export type Geometry = {
  x: number;
  y: number;
  width: number;
  height: number;
};

const frameWidth = 1280;
const frameHeight = 720;
const frameInset = 28;
const tileGap = 18;

export function gridGeometry(position: number, count: number): Geometry {
  const participantCount = Math.max(1, count);
  const columns = Math.ceil(Math.sqrt(participantCount * 16 / 9));
  const rows = Math.ceil(participantCount / columns);
  const width = Math.floor(
    (frameWidth - frameInset * 2 - tileGap * (columns - 1)) / columns,
  );
  const height = Math.floor(
    (frameHeight - frameInset * 2 - tileGap * (rows - 1)) / rows,
  );

  return {
    x: frameInset + (position % columns) * (width + tileGap),
    y: frameInset + Math.floor(position / columns) * (height + tileGap),
    width,
    height,
  };
}

export function screenLayout(count: number): {
  stage: Geometry;
  tiles: Geometry[];
} {
  if (count <= 2) {
    const tileSize = 244;
    const railX = frameWidth - frameInset - tileSize;
    const occupiedHeight = count * tileSize + Math.max(0, count - 1) * tileGap;
    const startY = Math.floor((frameHeight - occupiedHeight) / 2);

    return {
      stage: {
        x: frameInset,
        y: frameInset,
        width: railX - tileGap - frameInset,
        height: frameHeight - frameInset * 2,
      },
      tiles: Array.from({ length: count }, (_, index) => ({
        x: railX,
        y: startY + index * (tileSize + tileGap),
        width: tileSize,
        height: tileSize,
      })),
    };
  }

  if (count <= 4) {
    const tileSize = 150;
    const positions = [
      { x: 28, y: 133 },
      { x: 1102, y: 133 },
      { x: 28, y: 437 },
      { x: 1102, y: 437 },
    ];

    return {
      stage: { x: 206, y: 28, width: 868, height: 664 },
      tiles: positions.slice(0, count).map((position) => ({
        ...position,
        width: tileSize,
        height: tileSize,
      })),
    };
  }

  const tileSize = 136;
  const visibleCount = Math.min(count, 8);
  const totalWidth = visibleCount * tileSize + (visibleCount - 1) * tileGap;
  const startX = Math.floor(Math.max(frameInset, (frameWidth - totalWidth) / 2));

  return {
    stage: { x: 28, y: 28, width: 1224, height: 470 },
    tiles: Array.from({ length: visibleCount }, (_, index) => ({
      x: startX + index * (tileSize + tileGap),
      y: 556,
      width: tileSize,
      height: tileSize,
    })),
  };
}

export function initials(displayName: string): string {
  const parts = displayName
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .slice(0, 2);

  if (parts.length === 0) {
    return "?";
  }

  return parts
    .map((part) => [...part][0] ?? "")
    .join("")
    .toUpperCase();
}

export function truncateLabel(value: string, maximum: number): string {
  const characters = [...value];
  if (characters.length <= maximum) {
    return value;
  }

  return `${characters.slice(0, maximum - 1).join("")}…`;
}

export function escapeDrawText(value: string): string {
  let result = "";
  for (const character of value) {
    const codePoint = character.codePointAt(0);
    const unsafeCodePoint = codePoint === undefined
      || codePoint < 32
      || codePoint === 127;
    const safeCharacter = unsafeCodePoint ? " " : character;
    result += "\\':,;[]=".includes(safeCharacter)
      ? `\\${safeCharacter}`
      : safeCharacter;
  }
  return result;
}
