import { jidNormalizedUser } from "baileys";
import { StoreService } from "../providers/store/store-service.js";

export class JidResolver {
  constructor(private readonly storeService: StoreService | null) {}

  static normalizeJid(jid: string): string {
    if (!jid) return jid;
    if (typeof jidNormalizedUser === "function") {
      return jidNormalizedUser(jid);
    }
    return jid;
  }

  static isLidJid(jid: string): boolean {
    return Boolean(jid && String(jid).endsWith("@lid"));
  }

  static isPnJid(jid: string): boolean {
    return Boolean(jid && String(jid).endsWith("@s.whatsapp.net"));
  }

  static normalizePnNumber(value: string | null | undefined): string | null {
    if (!value) return null;
    const cleaned = String(value).replace(/[^\d]/g, "");
    return cleaned || null;
  }

  storeLidMapping(lidJid: string, pnJid: string | null): void {
    if (!this.storeService || !lidJid) return;
    const pnNumber = pnJid ? JidResolver.normalizePnNumber(pnJid) : null;
    this.storeService.upsertLidMapping(lidJid, pnJid, pnNumber);
  }

  storeLidMappingFromPair(a?: string, b?: string): void {
    const first = a || "";
    const second = b || "";
    if (!first || !second) return;
    if (JidResolver.isLidJid(first) && JidResolver.isPnJid(second)) {
      this.storeLidMapping(first, second);
    } else if (JidResolver.isPnJid(first) && JidResolver.isLidJid(second)) {
      this.storeLidMapping(second, first);
    }
  }

  storeLidMappingFromKey(key: any): void {
    if (!key) return;
    this.storeLidMappingFromPair(key.remoteJid, key.remoteJidAlt);
    this.storeLidMappingFromPair(key.participant, key.participantAlt);
  }

  storeLidMappingFromContact(contact: any): void {
    if (!contact) return;
    const id = contact?.id || contact?.jid || null;
    const lid =
      contact?.lid ||
      (id && JidResolver.isLidJid(id) ? id : null) ||
      (contact?.lid?.user ? contact.lid.user : null);
    const pn =
      contact?.phoneNumber?.user ||
      contact?.phoneNumber?.number ||
      contact?.phoneNumber ||
      (id && JidResolver.isPnJid(id) ? id : null);
    if (lid && pn) {
      this.storeLidMapping(
        lid,
        JidResolver.isPnJid(pn) ? pn : `${pn}@s.whatsapp.net`,
      );
    }
  }

  resolveLookupJid(jid: string): string {
    const normalized = JidResolver.normalizeJid(jid);
    if (!this.storeService || !normalized) return normalized;
    const direct = this.storeService.getChatById(normalized);
    if (direct) return normalized;
    const lidFromPn = this.storeService.getLidForPn(normalized);
    if (lidFromPn) return lidFromPn;
    const pnNumber = JidResolver.normalizePnNumber(normalized);
    if (pnNumber) {
      const lid = this.storeService.getLidForPn(pnNumber);
      if (lid) return lid;
    }
    return normalized;
  }

  getRelatedJids(jid: string): string[] {
    const normalized = JidResolver.normalizeJid(jid);
    if (!normalized) return [];
    if (!this.storeService) return [normalized];
    const related = new Set<string>();
    related.add(normalized);
    if (JidResolver.isLidJid(normalized)) {
      const mapped = this.storeService.getPnForLid(normalized);
      if (mapped?.pnJid) related.add(mapped.pnJid);
    } else {
      const lidFromPn = this.storeService.getLidForPn(normalized);
      if (lidFromPn) {
        related.add(lidFromPn);
      } else {
        const pnNumber = JidResolver.normalizePnNumber(normalized);
        if (pnNumber) {
          const lid = this.storeService.getLidForPn(pnNumber);
          if (lid) related.add(lid);
        }
      }
    }
    return Array.from(related);
  }

  resolveCanonicalChatId(jid: string): string {
    const normalized = JidResolver.normalizeJid(jid);
    if (!this.storeService || !normalized) return normalized;
    if (JidResolver.isLidJid(normalized)) {
      const mapped = this.storeService.getPnForLid(normalized);
      if (mapped?.pnJid) return mapped.pnJid;
    }
    return normalized;
  }
}
