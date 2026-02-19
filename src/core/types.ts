export interface SimpleContact {
  id: string;
  name: string | null;
  pushname: string | null;
  isMe: boolean;
  isUser: boolean;
  isGroup: boolean;
  isWAContact: boolean;
  isMyContact: boolean;
  number: string;
}

export interface ResolvedContact extends SimpleContact {
  matchedBy: "name" | "pushname" | "number" | "id";
  score: number;
}

export interface SimpleChat {
  id: string;
  name: string;
  isGroup: boolean;
  lastMessage?: SimpleMessage;
  unreadCount: number;
  timestamp: number;
}

export interface SimpleMessage {
  id: string;
  body: string;
  from: string;
  to: string;
  timestamp: number;
  fromMe: boolean;
  hasMedia: boolean;
  mediaKey?: string;
  type: string;
}
