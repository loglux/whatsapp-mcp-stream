import { log } from "../utils/logger.js";
import { StoreService } from "../providers/store/store-service.js";

export class WhatsAppSync {
  private warmupTimer: NodeJS.Timeout | null = null;
  private warmupAttempts = 0;
  private forcedResync = false;
  private lastHistorySyncAt: number | null = null;
  private lastChatsSyncAt: number | null = null;
  private lastMessagesSyncAt: number | null = null;

  constructor(
    private readonly storeService: StoreService | null,
    private readonly getSocket: () => any | null,
    private readonly normalizeChatRecord: (chat: any) => any | null,
    private readonly trackMessage: (msg: any) => void,
    private readonly upsertStoredChat: (chat: any) => void,
    private readonly upsertStoredMessage: (msg: any) => void,
    private readonly loadMessagesFromStore: (
      jid: string,
      limit: number,
    ) => Promise<any[]>,
    private readonly getChatsFromStore: () => any[],
  ) {}

  getStats() {
    return {
      lastHistorySyncAt: this.lastHistorySyncAt,
      lastChatsSyncAt: this.lastChatsSyncAt,
      lastMessagesSyncAt: this.lastMessagesSyncAt,
      warmupAttempts: this.warmupAttempts,
      warmupInProgress: Boolean(this.warmupTimer),
    };
  }

  markChatsSynced(count: number) {
    this.lastChatsSyncAt = Date.now();
    log.info({ count }, "Chats synced");
  }

  markMessagesSynced(count: number) {
    this.lastMessagesSyncAt = Date.now();
    log.info({ count }, "Messages synced");
  }

  markHistorySynced(chats: number, contacts: number, messages: number) {
    this.lastHistorySyncAt = Date.now();
    log.info(
      {
        chats,
        contacts,
        messages,
      },
      "Messaging history synced",
    );
  }

  scheduleWarmup(onForceResync?: () => Promise<void>) {
    if (this.warmupTimer) return;
    this.warmupAttempts = 0;
    this.warmupTimer = setInterval(() => {
      this.warmupAttempts += 1;
      const chatCount = this.getChatsFromStore().length;
      log.info({ attempt: this.warmupAttempts, chatCount }, "Warmup tick");
      if (chatCount > 0 || this.warmupAttempts > 5) {
        this.clearWarmupTimer();
        return;
      }
      this.warmup(onForceResync).catch((err) =>
        log.warn({ err }, "Warmup failed"),
      );
    }, 10000);
  }

  async runWarmup(
    getMessageCount?: () => number,
    onForceResync?: () => Promise<void>,
  ): Promise<{ chatCount: number; messageCount: number }> {
    await this.warmup(onForceResync);
    const chatCount = this.getChatsFromStore().length;
    const messageCount = getMessageCount ? getMessageCount() : 0;
    return { chatCount, messageCount };
  }

  clearWarmupTimer() {
    if (this.warmupTimer) {
      clearInterval(this.warmupTimer);
      this.warmupTimer = null;
    }
  }

  async forceResync(resetAppState: () => Promise<void>) {
    this.forcedResync = true;
    await resetAppState();
  }

  async warmup(onForceResync?: () => Promise<void>): Promise<void> {
    const sock = this.getSocket();
    if (!sock) return;
    try {
      log.info("Warmup started");
      const initialChats = this.getChatsFromStore();
      if (
        (!initialChats || initialChats.length === 0) &&
        typeof sock.resyncAppState === "function"
      ) {
        try {
          await sock.resyncAppState(undefined, true);
          log.info("Warmup requested app state resync");
        } catch (error) {
          log.warn({ err: error }, "Warmup app state resync failed");
        }
      }

      const chats = this.getChatsFromStore();
      if (chats?.length) {
        const targets = chats
          .map((c: any) => c?.id || c?.jid)
          .filter((jid: string) => Boolean(jid))
          .slice(0, 10);
        for (const jid of targets) {
          const msgs = await this.loadMessagesFromStore(jid, 20);
          if (msgs?.length) {
            msgs.forEach((msg: any) => this.trackMessage(msg));
          }
        }
      }
      const chatCount = chats?.length || 0;
      log.info({ chatCount }, "Warmup completed");
      if (chatCount === 0 && !this.forcedResync && this.warmupAttempts >= 2) {
        log.warn("Warmup still empty, forcing resync");
        this.forcedResync = true;
        if (onForceResync) {
          await onForceResync();
        }
      }
    } catch (error) {
      log.warn({ err: error }, "Warmup failed");
    }
  }

  handleChatsSet(payload: any, store: any) {
    if (payload?.chats && Array.isArray(payload.chats)) {
      const validChats = payload.chats
        .map((chat: any) => this.normalizeChatRecord(chat))
        .filter((chat: any) => chat?.id);
      if (validChats.length && store?.chats?.insert) {
        try {
          store.chats.insert(validChats);
        } catch (error) {
          log.warn({ err: error }, "Failed to insert chats into store");
        }
      }
      for (const chat of validChats) {
        this.upsertStoredChat(chat);
      }
      this.lastChatsSyncAt = Date.now();
      log.info({ count: validChats.length }, "Chats synced");
    }
  }

  handleMessagesSet(payload: any) {
    if (payload?.messages && Array.isArray(payload.messages)) {
      for (const msg of payload.messages) {
        this.trackMessage(msg);
        this.upsertStoredMessage(msg);
      }
      this.lastMessagesSyncAt = Date.now();
      log.info({ count: payload.messages.length }, "Messages synced");
    }
  }

  handleChatsUpsert(payload: any) {
    if (payload && Array.isArray(payload)) {
      for (const chat of payload) {
        this.upsertStoredChat(chat);
      }
      this.lastChatsSyncAt = Date.now();
      log.info({ count: payload.length }, "Chats upsert");
    }
  }

  handleChatsUpdate(payload: any) {
    if (payload && Array.isArray(payload)) {
      for (const chat of payload) {
        this.upsertStoredChat(chat);
      }
      this.lastChatsSyncAt = Date.now();
      log.info({ count: payload.length }, "Chats update");
    }
  }

  handleMessagingHistorySet(payload: any, store: any) {
    const { chats, contacts, messages } = payload || {};
    if (chats && Array.isArray(chats)) {
      const validChats = chats
        .map((chat: any) => this.normalizeChatRecord(chat))
        .filter((chat: any) => chat?.id);
      if (validChats.length && store?.chats?.insert) {
        try {
          store.chats.insert(validChats);
        } catch (error) {
          log.warn({ err: error }, "Failed to insert history chats into store");
        }
      }
      for (const chat of validChats) {
        this.upsertStoredChat(chat);
      }
    }
    if (contacts && store?.contacts) {
      for (const contact of contacts) {
        if (contact?.id) {
          store.contacts[contact.id] = contact;
        }
      }
    }
    if (messages && Array.isArray(messages)) {
      for (const msg of messages) {
        this.trackMessage(msg);
        this.upsertStoredMessage(msg);
      }
    }
    this.lastHistorySyncAt = Date.now();
    log.info(
      {
        chats: Array.isArray(chats) ? chats.length : 0,
        contacts: Array.isArray(contacts) ? contacts.length : 0,
        messages: Array.isArray(messages) ? messages.length : 0,
      },
      "Messaging history synced",
    );
  }

  getStoreStats() {
    return this.storeService?.stats() ?? null;
  }
}
