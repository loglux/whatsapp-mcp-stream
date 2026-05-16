import { describe, it, expect } from "vitest";
import { ContactPresenter } from "../../src/services/contact-presenter.js";
import { JidResolver } from "../../src/services/jid-resolver.js";
import type { StoreService } from "../../src/providers/store/store-service.js";

class FakeStore {
  contacts = new Map<string, any>();
  groupMeta = new Map<string, { subject?: string | null }>();
  pnToLid = new Map<string, string>();
  lidToPn = new Map<
    string,
    { pnJid: string | null; pnNumber: string | null }
  >();

  getContactById(jid: string) {
    return this.contacts.get(jid) ?? null;
  }
  getGroupMeta(jid: string) {
    return this.groupMeta.get(jid) ?? null;
  }
  getLidForPn(pn: string) {
    return this.pnToLid.get(pn) ?? null;
  }
  getPnForLid(lid: string) {
    return this.lidToPn.get(lid) ?? null;
  }
  // Required by JidResolver.resolveCanonicalChatId path through getRelatedJids
  getChatById(_jid: string) {
    return null;
  }
}

function build(): {
  store: FakeStore;
  presenter: ContactPresenter;
  jidResolver: JidResolver;
} {
  const store = new FakeStore();
  const service = store as unknown as StoreService;
  const jidResolver = new JidResolver(service);
  const presenter = new ContactPresenter({
    storeService: service,
    jidResolver,
  });
  return { store, presenter, jidResolver };
}

describe("ContactPresenter.getContactSummary", () => {
  it("returns null when the contact is unknown", () => {
    const { presenter } = build();
    expect(presenter.getContactSummary("x@s.whatsapp.net")).toBeNull();
  });

  it("returns the stored contact when found directly", () => {
    const { store, presenter } = build();
    store.contacts.set("x@s.whatsapp.net", {
      name: "Alice",
      pushname: "Ally",
      number: "447",
      is_my_contact: 1,
    });
    expect(presenter.getContactSummary("x@s.whatsapp.net")).toEqual({
      name: "Alice",
      pushname: "Ally",
      number: "447",
      is_my_contact: true,
    });
  });

  it("follows LID → PN aliasing when the LID is not directly indexed", () => {
    const { store, presenter } = build();
    store.lidToPn.set("abc@lid", {
      pnJid: "x@s.whatsapp.net",
      pnNumber: "447",
    });
    store.contacts.set("x@s.whatsapp.net", { name: "Alice" });
    expect(presenter.getContactSummary("abc@lid")?.name).toBe("Alice");
  });

  it("follows PN → LID aliasing when the PN is not directly indexed", () => {
    const { store, presenter } = build();
    store.pnToLid.set("x@s.whatsapp.net", "abc@lid");
    store.contacts.set("abc@lid", { name: "Alice" });
    expect(presenter.getContactSummary("x@s.whatsapp.net")?.name).toBe("Alice");
  });
});

describe("ContactPresenter.isUsableDisplayName", () => {
  const { presenter } = build();
  it("rejects empty values", () => {
    expect(presenter.isUsableDisplayName(null)).toBe(false);
    expect(presenter.isUsableDisplayName("   ")).toBe(false);
  });
  it("rejects names that equal the canonical id", () => {
    expect(
      presenter.isUsableDisplayName("x@s.whatsapp.net", "x@s.whatsapp.net"),
    ).toBe(false);
  });
  it("rejects LID-shaped strings", () => {
    expect(presenter.isUsableDisplayName("abc@lid")).toBe(false);
  });
  it("accepts human-looking names", () => {
    expect(presenter.isUsableDisplayName("Alice")).toBe(true);
  });
});

describe("ContactPresenter.getBestContactName", () => {
  it("prefers name over pushname over number", () => {
    const { store, presenter } = build();
    store.contacts.set("a@s.whatsapp.net", {
      name: "Alice",
      pushname: "Ally",
      number: "447",
    });
    expect(presenter.getBestContactName("a@s.whatsapp.net")).toBe("Alice");
    store.contacts.set("b@s.whatsapp.net", {
      name: null,
      pushname: "Bob",
      number: "448",
    });
    expect(presenter.getBestContactName("b@s.whatsapp.net")).toBe("Bob");
    store.contacts.set("c@s.whatsapp.net", {
      name: null,
      pushname: null,
      number: "449",
    });
    expect(presenter.getBestContactName("c@s.whatsapp.net")).toBe("449");
  });
});

describe("ContactPresenter.getBestGroupName", () => {
  it("uses storedName when usable", () => {
    const { presenter } = build();
    expect(presenter.getBestGroupName([], "g@g.us", "My Group")).toBe(
      "My Group",
    );
  });

  it("falls through to group meta subject when storedName is unusable", () => {
    const { store, presenter } = build();
    store.groupMeta.set("g@g.us", { subject: "From Meta" });
    expect(presenter.getBestGroupName([], "g@g.us", null)).toBe("From Meta");
  });

  it("returns null when nothing usable is found", () => {
    const { presenter } = build();
    expect(presenter.getBestGroupName([], "g@g.us", null)).toBeNull();
  });
});

describe("ContactPresenter.getBestChatName", () => {
  it("uses group name for groups", () => {
    const { store, presenter } = build();
    store.groupMeta.set("g@g.us", { subject: "Team" });
    expect(presenter.getBestChatName([], "g@g.us", null)).toBe("Team");
  });

  it("falls back to the related PN number for DMs", () => {
    const { presenter } = build();
    expect(
      presenter.getBestChatName([], "447700900111@s.whatsapp.net", null),
    ).toBe("447700900111");
  });

  it("uses storedName when it is usable for DMs", () => {
    const { presenter } = build();
    expect(
      presenter.getBestChatName([], "x@s.whatsapp.net", "Custom Label"),
    ).toBe("Custom Label");
  });
});

describe("ContactPresenter.buildSimpleContact / mergeContacts", () => {
  it("buildSimpleContact derives number from the JID when missing", () => {
    const { presenter } = build();
    const r = presenter.buildSimpleContact({
      jid: "447700900111@s.whatsapp.net",
      name: "Alice",
    });
    expect(r.id).toBe("447700900111@s.whatsapp.net");
    expect(r.number).toBe("447700900111");
    expect(r.name).toBe("Alice");
  });

  it("mergeContacts deduplicates by canonical id and keeps best fields", () => {
    const { store, presenter } = build();
    // Set up canonicalisation: lid resolves to PN.
    store.lidToPn.set("abc@lid", {
      pnJid: "447@s.whatsapp.net",
      pnNumber: "447",
    });
    const merged = presenter.mergeContacts([
      { jid: "abc@lid", name: null, pushname: null, is_my_contact: 0 },
      {
        jid: "447@s.whatsapp.net",
        name: "Alice",
        pushname: "Ally",
        number: "447",
        is_my_contact: 1,
      },
    ]);
    expect(merged).toHaveLength(1);
    expect(merged[0].name).toBe("Alice");
    expect(merged[0].pushname).toBe("Ally");
    expect(merged[0].isMyContact).toBe(true);
  });
});
