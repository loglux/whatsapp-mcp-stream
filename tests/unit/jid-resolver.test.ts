import { describe, expect, it } from "vitest";
import { JidResolver } from "../../src/services/jid-resolver.js";
import type { StoreService } from "../../src/providers/store/store-service.js";

interface MappingArgs {
  lidJid: string;
  pnJid: string | null;
  pnNumber: string | null;
}

class MockStore {
  chats = new Set<string>();
  pnToLid = new Map<string, string>();
  lidToPn = new Map<
    string,
    { pnJid: string | null; pnNumber: string | null }
  >();
  mappings: MappingArgs[] = [];

  getChatById(jid: string): { id: string } | null {
    return this.chats.has(jid) ? { id: jid } : null;
  }

  getLidForPn(pnJidOrNumber: string): string | null {
    return this.pnToLid.get(pnJidOrNumber) ?? null;
  }

  getPnForLid(
    lidJid: string,
  ): { pnJid: string | null; pnNumber: string | null } | null {
    return this.lidToPn.get(lidJid) ?? null;
  }

  upsertLidMapping(
    lidJid: string,
    pnJid: string | null,
    pnNumber: string | null,
  ): void {
    this.mappings.push({ lidJid, pnJid, pnNumber });
  }
}

function freshResolver(): { store: MockStore; resolver: JidResolver } {
  const store = new MockStore();
  return {
    store,
    resolver: new JidResolver(store as unknown as StoreService),
  };
}

describe("JidResolver static helpers", () => {
  it("isLidJid only matches @lid suffix", () => {
    expect(JidResolver.isLidJid("abc@lid")).toBe(true);
    expect(JidResolver.isLidJid("abc@s.whatsapp.net")).toBe(false);
    expect(JidResolver.isLidJid("")).toBe(false);
  });

  it("isPnJid only matches @s.whatsapp.net suffix", () => {
    expect(JidResolver.isPnJid("447700900111@s.whatsapp.net")).toBe(true);
    expect(JidResolver.isPnJid("abc@lid")).toBe(false);
    expect(JidResolver.isPnJid("")).toBe(false);
  });

  it("normalizePnNumber strips non-digits, returns null for empty result", () => {
    expect(JidResolver.normalizePnNumber("+44 7700 900 111")).toBe(
      "447700900111",
    );
    expect(JidResolver.normalizePnNumber("abc")).toBeNull();
    expect(JidResolver.normalizePnNumber(null)).toBeNull();
    expect(JidResolver.normalizePnNumber(undefined)).toBeNull();
  });
});

describe("JidResolver storeLidMapping", () => {
  it("is a no-op when storeService is null", () => {
    const resolver = new JidResolver(null);
    resolver.storeLidMapping("x@lid", "447700900111@s.whatsapp.net");
    // No throw, nothing to assert beyond the absence of effects.
    expect(resolver).toBeDefined();
  });

  it("persists the mapping with derived pnNumber", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMapping("x@lid", "447700900111@s.whatsapp.net");
    expect(store.mappings).toEqual([
      {
        lidJid: "x@lid",
        pnJid: "447700900111@s.whatsapp.net",
        pnNumber: "447700900111",
      },
    ]);
  });

  it("persists with null pnNumber when pnJid is null", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMapping("x@lid", null);
    expect(store.mappings[0]).toEqual({
      lidJid: "x@lid",
      pnJid: null,
      pnNumber: null,
    });
  });
});

describe("JidResolver storeLidMappingFromPair", () => {
  it("stores when first is LID and second is PN", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMappingFromPair("x@lid", "447700900111@s.whatsapp.net");
    expect(store.mappings[0]).toMatchObject({
      lidJid: "x@lid",
      pnJid: "447700900111@s.whatsapp.net",
    });
  });

  it("flips when first is PN and second is LID", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMappingFromPair("447700900111@s.whatsapp.net", "x@lid");
    expect(store.mappings[0]).toMatchObject({
      lidJid: "x@lid",
      pnJid: "447700900111@s.whatsapp.net",
    });
  });

  it("ignores pairs that are not one-LID-one-PN", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMappingFromPair("x@lid", "y@lid");
    resolver.storeLidMappingFromPair("a@s.whatsapp.net", "b@s.whatsapp.net");
    resolver.storeLidMappingFromPair("x@lid", "");
    resolver.storeLidMappingFromPair(undefined, "x@lid");
    expect(store.mappings).toEqual([]);
  });
});

describe("JidResolver storeLidMappingFromKey", () => {
  it("extracts pairs from remoteJid/remoteJidAlt and participant/participantAlt", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMappingFromKey({
      remoteJid: "abc@lid",
      remoteJidAlt: "447700900111@s.whatsapp.net",
      participant: "447700900222@s.whatsapp.net",
      participantAlt: "def@lid",
    });
    expect(store.mappings).toHaveLength(2);
    expect(store.mappings[0].lidJid).toBe("abc@lid");
    expect(store.mappings[1].lidJid).toBe("def@lid");
  });

  it("handles null key without throwing", () => {
    const { resolver } = freshResolver();
    expect(() => resolver.storeLidMappingFromKey(null)).not.toThrow();
  });
});

describe("JidResolver storeLidMappingFromContact", () => {
  it("derives lid from id when id ends with @lid and pn from phoneNumber.user", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMappingFromContact({
      id: "abc@lid",
      phoneNumber: { user: "447700900111" },
    });
    expect(store.mappings[0]).toMatchObject({
      lidJid: "abc@lid",
      pnJid: "447700900111@s.whatsapp.net",
    });
  });

  it("uses contact.lid string when provided", () => {
    const { store, resolver } = freshResolver();
    resolver.storeLidMappingFromContact({
      id: "447700900111@s.whatsapp.net",
      lid: "def@lid",
    });
    expect(store.mappings[0]).toMatchObject({
      lidJid: "def@lid",
      pnJid: "447700900111@s.whatsapp.net",
    });
  });
});

describe("JidResolver resolveLookupJid", () => {
  it("returns the normalized jid when storeService is null", () => {
    const resolver = new JidResolver(null);
    expect(resolver.resolveLookupJid("447700900111@s.whatsapp.net")).toBe(
      "447700900111@s.whatsapp.net",
    );
  });

  it("keeps the jid when there is a direct chat", () => {
    const { store, resolver } = freshResolver();
    store.chats.add("abc@lid");
    expect(resolver.resolveLookupJid("abc@lid")).toBe("abc@lid");
  });

  it("returns the LID when the PN maps to one", () => {
    const { store, resolver } = freshResolver();
    store.pnToLid.set("447700900111@s.whatsapp.net", "abc@lid");
    expect(resolver.resolveLookupJid("447700900111@s.whatsapp.net")).toBe(
      "abc@lid",
    );
  });

  it("falls back to looking up by raw PN number", () => {
    const { store, resolver } = freshResolver();
    store.pnToLid.set("447700900111", "abc@lid");
    expect(resolver.resolveLookupJid("447700900111@s.whatsapp.net")).toBe(
      "abc@lid",
    );
  });
});

describe("JidResolver getRelatedJids", () => {
  it("returns just the normalized jid when there is no store", () => {
    const resolver = new JidResolver(null);
    expect(resolver.getRelatedJids("447700900111@s.whatsapp.net")).toEqual([
      "447700900111@s.whatsapp.net",
    ]);
  });

  it("includes the mapped PN for a LID", () => {
    const { store, resolver } = freshResolver();
    store.lidToPn.set("abc@lid", {
      pnJid: "447700900111@s.whatsapp.net",
      pnNumber: "447700900111",
    });
    expect(resolver.getRelatedJids("abc@lid").sort()).toEqual([
      "447700900111@s.whatsapp.net",
      "abc@lid",
    ]);
  });

  it("includes the mapped LID for a PN", () => {
    const { store, resolver } = freshResolver();
    store.pnToLid.set("447700900111@s.whatsapp.net", "abc@lid");
    expect(
      resolver.getRelatedJids("447700900111@s.whatsapp.net").sort(),
    ).toEqual(["447700900111@s.whatsapp.net", "abc@lid"]);
  });

  it("returns [] for empty input", () => {
    const { resolver } = freshResolver();
    expect(resolver.getRelatedJids("")).toEqual([]);
  });
});

describe("JidResolver resolveCanonicalChatId", () => {
  it("returns the PN when the LID maps to one", () => {
    const { store, resolver } = freshResolver();
    store.lidToPn.set("abc@lid", {
      pnJid: "447700900111@s.whatsapp.net",
      pnNumber: "447700900111",
    });
    expect(resolver.resolveCanonicalChatId("abc@lid")).toBe(
      "447700900111@s.whatsapp.net",
    );
  });

  it("returns the original LID when no mapping exists", () => {
    const { resolver } = freshResolver();
    expect(resolver.resolveCanonicalChatId("abc@lid")).toBe("abc@lid");
  });

  it("returns the PN unchanged for PN inputs", () => {
    const { resolver } = freshResolver();
    expect(resolver.resolveCanonicalChatId("447700900111@s.whatsapp.net")).toBe(
      "447700900111@s.whatsapp.net",
    );
  });
});
