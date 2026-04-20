export function formatHistoryDate(timestamp: number, timeZone = 'Europe/Vienna'): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(new Date(timestamp));

  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    throw new Error(`Could not format history date for timestamp ${timestamp}`);
  }

  return `${year}-${month}-${day}`;
}

export function historyPath(rootPath: string, fieldPath: string, timestamp: number): string {
  return joinFirebasePath(rootPath, 'history', formatHistoryDate(timestamp), fieldPath.replaceAll('.', '/'), String(timestamp));
}

export function currentPath(rootPath: string): string {
  return joinFirebasePath(rootPath, 'current');
}

export function joinFirebasePath(...parts: string[]): string {
  return parts
    .map((part) => part.trim().replace(/^\/+|\/+$/g, ''))
    .filter(Boolean)
    .join('/');
}
