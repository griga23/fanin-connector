package com.example.fanin;

/**
 * The control state of one source partition as the task last applied it. The task writes it into the metadata of
 * the committed offset (see {@link FanInSinkTask#preCommit}), so it survives a rebalance, a restart and the
 * compaction of the control topic. Formats written: {@code fanin.ctl:v1:allowed:<add time>},
 * {@code fanin.ctl:v1:removed:<remove time>} and {@code fanin.ctl:v2:removed:<remove time>:<remove cut>}.
 *
 * <p>The form with a remove cut has its own prefix v2, because a build before the time-based remove reads
 * {@code v1:removed:} with anything after the time as no ledger at all. With v2 such a build also sees no ledger, which
 * it handles as a partition without a ledger (documented in docs/CONTROL-TOPIC.md, Limits). The parser reads all
 * forms under both prefixes, also {@code v1:removed:<t>:<cut>} as written by the first build of the time-based remove.
 *
 * <p>"allowed" means: the cut for this add was applied, and the committed offset is at or after it.
 *
 * <p>"removed" with a remove cut means: the topic was removed at that time. Records of this partition below the cut
 * offset were written before the remove and are forwarded; records at or after it are not. The next add must cut
 * again.
 *
 * <p>"removed" without a cut (written by an older build, or when the task does not know the cut) means: no record
 * after the committed offset is forwarded before the next add. The next add must cut again.
 *
 * <p>Times and cuts are decimal digits only (no sign). A "removed" ledger with a valid time and a cut that is not
 * valid is read as "removed" without a cut: it forwards nothing more, which is the safe reading.
 */
record ControlLedger(boolean allowed, long timeMs, long cut) {

    static final String PREFIX = "fanin.ctl:v1:";
    /** Prefix of the form with a remove cut. */
    static final String PREFIX_V2 = "fanin.ctl:v2:";
    private static final String ALLOWED = "allowed:";
    private static final String REMOVED = "removed:";
    /** Value of {@link #cut} when there is no remove cut. */
    static final long NO_CUT = -1L;

    static ControlLedger allowedAt(long addTimeMs) {
        return new ControlLedger(true, addTimeMs, NO_CUT);
    }

    /** A remove without a known cut: nothing after the committed offset is forwarded. */
    static ControlLedger removedAt(long removeTimeMs) {
        return new ControlLedger(false, removeTimeMs, NO_CUT);
    }

    /** A remove whose records below {@code cutOffset} are forwarded. */
    static ControlLedger removedAt(long removeTimeMs, long cutOffset) {
        return new ControlLedger(false, removeTimeMs, cutOffset);
    }

    boolean hasCut() {
        return !allowed && cut >= 0;
    }

    String encode() {
        if (allowed) {
            return PREFIX + ALLOWED + timeMs;
        }
        return hasCut() ? PREFIX_V2 + REMOVED + timeMs + ":" + cut : PREFIX + REMOVED + timeMs;
    }

    /** The ledger in committed offset metadata, or null if the metadata is empty or not a valid ledger. */
    static ControlLedger parse(String metadata) {
        String rest;
        if (metadata == null) {
            return null;
        } else if (metadata.startsWith(PREFIX)) {
            rest = metadata.substring(PREFIX.length());
        } else if (metadata.startsWith(PREFIX_V2)) {
            rest = metadata.substring(PREFIX_V2.length());
        } else {
            return null;
        }
        if (rest.startsWith(ALLOWED)) {
            long time = number(rest.substring(ALLOWED.length()));
            return time < 0 ? null : allowedAt(time);
        }
        if (rest.startsWith(REMOVED)) {
            String body = rest.substring(REMOVED.length());
            int colon = body.indexOf(':');
            long time = number(colon < 0 ? body : body.substring(0, colon));
            if (time < 0) {
                return null;
            }
            if (colon < 0) {
                return removedAt(time);
            }
            long cut = number(body.substring(colon + 1));
            return cut < 0 ? removedAt(time) : removedAt(time, cut);
        }
        return null;
    }

    /** The value of a string of decimal digits, or -1 if it is empty, has another character or is too large. */
    private static long number(String s) {
        if (s.isEmpty() || s.length() > 19) {
            return -1;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
