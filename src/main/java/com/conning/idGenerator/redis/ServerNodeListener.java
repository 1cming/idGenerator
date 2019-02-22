package com.conning.idGenerator.redis;

public abstract interface ServerNodeListener {
	public abstract void eventOccured(ServerNodeEventType paramServerNodeEventType, String paramString);

	public static enum ServerNodeEventType {
		UPDATE, REMOVE;

		private ServerNodeEventType() {
		}
	}
}