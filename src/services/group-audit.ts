import { SimpleChat } from "../core/types.js";
import { log } from "../utils/logger.js";
import { StoreService } from "../providers/store/store-service.js";

export interface GroupAuditDeps {
  storeService: StoreService | null;
  listGroups: (limit: number, refresh: boolean) => Promise<SimpleChat[]>;
  getGroupInfo: (
    jid: string,
  ) => Promise<{ participants?: Array<{ id?: string }> } | null>;
  resolveCanonicalChatId: (jid: string) => string;
  getRelatedJids: (jid: string) => string[];
  normalizeJid: (jid: string) => string;
  normalizePnNumber: (jid: string) => string | null;
  getContactSummary: (jid: string) => {
    name: string | null;
    pushname: string | null;
    number: string | null;
  } | null;
}

export type AuditMember = {
  canonicalId: string;
  ids: string[];
  name: string | null;
  pushname: string | null;
  number: string | null;
  groupCount: number;
  groups: Array<{ id: string; name: string }>;
  hasDirectChat: boolean;
  inContacts: boolean;
};

export type AuditMatrix = {
  groupsProcessed: number;
  members: AuditMember[];
};

export class GroupAuditEngine {
  constructor(private readonly deps: GroupAuditDeps) {}

  private hasDirectChatForParticipant(jid: string): boolean {
    if (!this.deps.storeService || !jid) return false;
    const related = this.deps.getRelatedJids(jid);
    for (const entry of related) {
      const chat = this.deps.storeService.getChatById(entry);
      if (chat && !chat.is_group) return true;
    }
    return false;
  }

  private isParticipantInContacts(jid: string): boolean {
    if (!this.deps.storeService || !jid) return false;
    const related = this.deps.getRelatedJids(jid);
    for (const entry of related) {
      if (this.deps.storeService.getContactById(entry)) return true;
    }
    return false;
  }

  private async getGroupParticipantJids(
    groupJid: string,
    refresh: boolean,
  ): Promise<string[]> {
    if (!groupJid) return [];
    if (refresh) {
      const info = await this.deps.getGroupInfo(groupJid);
      const participants = Array.isArray(info?.participants)
        ? info.participants
        : [];
      return participants
        .map((p) => String(p?.id || "").trim())
        .filter(Boolean);
    }
    if (this.deps.storeService) {
      const cached = this.deps.storeService.listGroupParticipants(groupJid);
      if (cached.length > 0) {
        return cached
          .map((p) => String(p.participant_jid || "").trim())
          .filter(Boolean);
      }
    }
    return [];
  }

  private buildMemberDisplay(canonicalId: string): {
    name: string | null;
    pushname: string | null;
    number: string | null;
  } {
    const summary = this.deps.getContactSummary(canonicalId);
    return {
      name: summary?.name || null,
      pushname: summary?.pushname || null,
      number:
        summary?.number || this.deps.normalizePnNumber(canonicalId) || null,
    };
  }

  private async buildMatrix(
    groupLimit: number,
    refresh: boolean,
  ): Promise<AuditMatrix> {
    let groups: SimpleChat[] = [];
    try {
      groups = await this.deps.listGroups(groupLimit, false);
    } catch (error) {
      log.warn(
        { err: error, groupLimit, refreshGroupInfo: refresh },
        "Group audit: failed to list groups",
      );
      return { groupsProcessed: 0, members: [] };
    }

    const memberMap = new Map<
      string,
      {
        canonicalId: string;
        ids: Set<string>;
        groups: Array<{ id: string; name: string }>;
      }
    >();

    for (const group of groups) {
      const groupId = String(group.id || "").trim();
      if (!groupId.endsWith("@g.us")) continue;
      let participants: string[] = [];
      try {
        participants = await this.getGroupParticipantJids(groupId, refresh);
      } catch (error) {
        log.warn(
          { err: error, groupId, refreshGroupInfo: refresh },
          "Group audit: failed to load participants",
        );
        continue;
      }
      for (const raw of participants) {
        const participant = this.deps.normalizeJid(raw);
        if (!participant) continue;
        const canonicalId = this.deps.resolveCanonicalChatId(participant);
        const existing = memberMap.get(canonicalId) || {
          canonicalId,
          ids: new Set<string>(),
          groups: [],
        };
        existing.ids.add(participant);
        if (!existing.groups.some((g) => g.id === groupId)) {
          existing.groups.push({ id: groupId, name: group.name || groupId });
        }
        memberMap.set(canonicalId, existing);
      }
    }

    const members: AuditMember[] = Array.from(memberMap.values())
      .map((entry) => {
        const profile = this.buildMemberDisplay(entry.canonicalId);
        return {
          canonicalId: entry.canonicalId,
          ids: Array.from(entry.ids.values()).sort(),
          name: profile.name,
          pushname: profile.pushname,
          number: profile.number,
          groupCount: entry.groups.length,
          groups: entry.groups.sort((a, b) => a.name.localeCompare(b.name)),
          hasDirectChat: this.hasDirectChatForParticipant(entry.canonicalId),
          inContacts: this.isParticipantInContacts(entry.canonicalId),
        };
      })
      .sort(
        (a, b) =>
          b.groupCount - a.groupCount ||
          String(a.name || "").localeCompare(String(b.name || "")),
      );

    return { groupsProcessed: groups.length, members };
  }

  async analyzeOverlaps(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 2,
  ): Promise<{
    groupsProcessed: number;
    totalMembers: number;
    overlaps: AuditMember[];
  }> {
    const matrix = await this.buildMatrix(groupLimit, refreshGroupInfo);
    const threshold = Math.max(2, minSharedGroups);
    const overlaps = matrix.members.filter((m) => m.groupCount >= threshold);
    return {
      groupsProcessed: matrix.groupsProcessed,
      totalMembers: matrix.members.length,
      overlaps,
    };
  }

  async findWithoutDirectChat(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 1,
  ): Promise<{
    groupsProcessed: number;
    totalMembers: number;
    members: AuditMember[];
  }> {
    const matrix = await this.buildMatrix(groupLimit, refreshGroupInfo);
    const threshold = Math.max(1, minSharedGroups);
    const members = matrix.members.filter(
      (m) => !m.hasDirectChat && m.groupCount >= threshold,
    );
    return {
      groupsProcessed: matrix.groupsProcessed,
      totalMembers: matrix.members.length,
      members,
    };
  }

  async findNotInContacts(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 1,
  ): Promise<{
    groupsProcessed: number;
    totalMembers: number;
    members: AuditMember[];
  }> {
    const matrix = await this.buildMatrix(groupLimit, refreshGroupInfo);
    const threshold = Math.max(1, minSharedGroups);
    const members = matrix.members.filter(
      (m) => !m.inContacts && m.groupCount >= threshold,
    );
    return {
      groupsProcessed: matrix.groupsProcessed,
      totalMembers: matrix.members.length,
      members,
    };
  }

  async runAudit(
    groupLimit = 200,
    refreshGroupInfo = false,
    overlapMinSharedGroups = 2,
    minSharedGroups = 1,
  ): Promise<{
    summary: {
      groupsProcessed: number;
      totalMembers: number;
      overlapCount: number;
      withoutDirectChatCount: number;
      notInContactsCount: number;
    };
    overlaps: AuditMember[];
    withoutDirectChat: AuditMember[];
    notInContacts: AuditMember[];
  }> {
    const matrix = await this.buildMatrix(groupLimit, refreshGroupInfo);
    const overlapThreshold = Math.max(2, overlapMinSharedGroups);
    const baseThreshold = Math.max(1, minSharedGroups);
    const overlaps = matrix.members.filter(
      (m) => m.groupCount >= overlapThreshold,
    );
    const withoutDirectChat = matrix.members.filter(
      (m) => !m.hasDirectChat && m.groupCount >= baseThreshold,
    );
    const notInContacts = matrix.members.filter(
      (m) => !m.inContacts && m.groupCount >= baseThreshold,
    );
    return {
      summary: {
        groupsProcessed: matrix.groupsProcessed,
        totalMembers: matrix.members.length,
        overlapCount: overlaps.length,
        withoutDirectChatCount: withoutDirectChat.length,
        notInContactsCount: notInContacts.length,
      },
      overlaps,
      withoutDirectChat,
      notInContacts,
    };
  }
}
