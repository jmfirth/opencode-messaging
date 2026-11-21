/**
 * Narrow structural types shared by the lib modules.
 *
 * The host's own schemas are effect-schema derived and describe far more than
 * this plugin touches; these interfaces name only the fields this plugin
 * reads, so a shape mismatch shows up at the boundary instead of being smeared
 * across every call site as `any`.
 */

/** A session as seen through the service API or the TUI data cache. */
export interface SessionLike {
  readonly id: string
  readonly parentID?: string
  readonly agent?: string
  readonly title?: string
  readonly location?: { readonly directory?: string }
  readonly time?: { readonly updated?: number; readonly created?: number }
}

/** The per-call tool context: who is sending, and how to ask for permission. */
export interface SenderContext {
  readonly sessionID: string
  readonly agent: string
  readonly messageID?: string
  readonly id?: string
}

/** Plugin options, as configured for opencode-messaging. */
export interface OptionsSource {
  readonly options?: Record<string, unknown>
}
