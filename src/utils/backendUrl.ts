const DEFAULT_BACKEND_URL = 'http://localhost:3000';

export function getBackendBaseUrl(): string {
  const raw = process.env.BACKEND_URL || process.env.API_URL || DEFAULT_BACKEND_URL;
  const trimmed = raw.trim();

  if (!trimmed) return DEFAULT_BACKEND_URL;
  if (/^https?:\/\//i.test(trimmed)) return trimmed.replace(/\/+$/, '');

  // Allow shorthand host values like "api.videomanch.com" in env.
  return `https://${trimmed.replace(/\/+$/, '')}`;
}

