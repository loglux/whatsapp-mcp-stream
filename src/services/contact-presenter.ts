import { SimpleContact } from "../core/types.js";
import { JidResolver } from "./jid-resolver.js";
import type { StoreService } from "../providers/store/store-service.js";

export interface ContactSummary {
  name: string | null;
  pushname: string | null;
  number: string | null;
  is_my_contact: boolean | null;
}

export interface ContactPresenterDeps {
  storeService: StoreService | null;
  jidResolver: JidResolver;
}

/**
 * Display-side helpers for contacts and chats. Reads from the SQLite store
 * with LID/PN aliasing, picks the most usable display name for a chat, and
 * normalises raw contact rows into the public `SimpleContact` shape consumed
 * by MCP responses.
 */
export class ContactPresenter {
  constructor(private readonly deps: ContactPresenterDeps) {}

  getContactSummary(jid: string): ContactSummary | null {
    const store = this.deps.storeService;
    if (!store || !jid) return null;
    let contact = store.getContactById(jid);
    if (!contact && JidResolver.isLidJid(jid)) {
      const mapped = store.getPnForLid(jid);
      if (mapped?.pnJid) {
        contact = store.getContactById(mapped.pnJid);
      }
    }
    if (!contact && JidResolver.isPnJid(jid)) {
      const lid = store.getLidForPn(jid);
      if (lid) {
        contact = store.getContactById(lid);
      }
    }
    if (!contact) return null;
    return {
      name: contact.name || null,
      pushname: contact.pushname || null,
      number: contact.number || null,
      is_my_contact: contact.is_my_contact ? true : false,
    };
  }

  getBestContactName(jid: string): string | null {
    const summary = this.getContactSummary(jid);
    if (!summary) return null;
    const candidates = [summary.name, summary.pushname, summary.number];
    for (const candidate of candidates) {
      if (this.isUsableDisplayName(candidate, jid)) {
        return candidate!;
      }
    }
    return null;
  }

  getBestChatName(
    related: string[],
    canonicalId: string,
    storedName: string | null | undefined,
  ): string | null {
    if (canonicalId.endsWith("@g.us")) {
      const groupName = this.getBestGroupName(related, canonicalId, storedName);
      if (groupName) return groupName;
    }
    if (this.isUsableDisplayName(storedName, canonicalId)) return storedName!;
    for (const jid of related) {
      const name = this.getBestContactName(jid);
      if (name) return name;
    }
    const fallback = this.getBestContactName(canonicalId);
    if (fallback) return fallback;
    if (JidResolver.isPnJid(canonicalId)) {
      const pn = JidResolver.normalizePnNumber(canonicalId);
      if (pn) return pn;
    }
    return this.isUsableDisplayName(storedName, canonicalId)
      ? storedName!
      : null;
  }

  getBestGroupName(
    related: string[],
    canonicalId: string,
    storedName: string | null | undefined,
  ): string | null {
    const candidates: Array<string | null | undefined> = [storedName];
    const store = this.deps.storeService;
    if (store) {
      for (const jid of [canonicalId, ...related]) {
        const meta = store.getGroupMeta(jid);
        if (meta?.subject) {
          candidates.push(meta.subject);
        }
      }
    }
    for (const candidate of candidates) {
      if (this.isUsableDisplayName(candidate, canonicalId)) {
        return candidate!;
      }
    }
    return null;
  }

  isUsableDisplayName(
    value: string | null | undefined,
    canonicalId?: string,
  ): boolean {
    if (!value) return false;
    const trimmed = String(value).trim();
    if (!trimmed) return false;
    if (trimmed === canonicalId) return false;
    if (JidResolver.isLidJid(trimmed)) return false;
    return true;
  }

  buildSimpleContact(contact: any, canonicalId?: string): SimpleContact {
    const jid = contact?.jid || contact?.id || "";
    const id = canonicalId || jid;
    const number =
      contact?.number ||
      JidResolver.normalizePnNumber(jid) ||
      JidResolver.normalizePnNumber(canonicalId || "") ||
      "";
    return {
      id,
      name: contact?.name || null,
      pushname: contact?.pushname || null,
      isMe: false,
      isUser: true,
      isGroup: Boolean(contact?.is_group),
      isWAContact: true,
      isMyContact: Boolean(contact?.is_my_contact),
      number,
    };
  }

  mergeContacts(contacts: any[]): SimpleContact[] {
    const merged = new Map<string, SimpleContact>();
    for (const contact of contacts) {
      const jid = contact?.jid || contact?.id || "";
      if (!jid) continue;
      const canonicalId = this.deps.jidResolver.resolveCanonicalChatId(jid);
      const entry = this.buildSimpleContact(contact, canonicalId);
      const existing = merged.get(canonicalId);
      if (!existing) {
        merged.set(canonicalId, entry);
        continue;
      }
      const name =
        existing.name && existing.name !== existing.id
          ? existing.name
          : entry.name && entry.name !== entry.id
            ? entry.name
            : existing.name || entry.name;
      const pushname = existing.pushname || entry.pushname;
      const number = existing.number || entry.number;
      merged.set(canonicalId, {
        ...existing,
        name,
        pushname,
        number,
        isMyContact: existing.isMyContact || entry.isMyContact,
      });
    }
    return Array.from(merged.values());
  }
}
