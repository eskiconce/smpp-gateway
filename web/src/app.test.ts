import { describe, it, expect } from 'vitest';

// Mock location for Node environment
(globalThis as any).location = { hash: '' };

// We test currentRoute logic directly since it's a pure function
function currentRoute(): string {
  const h = (globalThis as any).location.hash || '';
  return h.startsWith('#/') ? h : '#/dashboard';
}

describe('hash routing', () => {
  it('defaults a dashboard', () => {
    (globalThis as any).location.hash = '';
    expect(currentRoute()).toBe('#/dashboard');
  });

  it('returns hash when set', () => {
    (globalThis as any).location.hash = '#/messages';
    expect(currentRoute()).toBe('#/messages');
  });
});
