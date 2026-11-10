import { useState } from "react";
import { api, type GenieAnswer } from "../api";

type Turn = { question: string; answer: GenieAnswer | null; error: string | null };

export default function GeniePanel({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [question, setQuestion] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [conversation, setConversation] = useState<string | undefined>();
  const [busy, setBusy] = useState(false);

  async function send() {
    const q = question.trim();
    if (!q || busy) return;
    setBusy(true);
    setQuestion("");
    setTurns((t) => [...t, { question: q, answer: null, error: null }]);
    const update = (patch: Partial<Turn>) => setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { ...x, ...patch } : x)));
    try {
      const { conversation_id, message_id } = await api.ask(q, conversation);
      setConversation(conversation_id);
      for (let attempt = 0; attempt < 120; attempt++) {
        const answer = await api.poll(conversation_id, message_id);
        update({ answer });
        if (answer.done) break;
        await new Promise((r) => setTimeout(r, 1500));
      }
    } catch (e) {
      update({ error: (e as Error).message });
    } finally {
      setBusy(false);
    }
  }

  if (!open) return null;
  return (
    <aside className="genie">
      <header><h2>Ask Genie</h2><button onClick={onClose}>✕</button></header>
      <div className="turns">
        {turns.length === 0 && <p className="muted">Try: "Which facilities are crowded right now?" or "Where is boarding worst?"</p>}
        {turns.map((t, i) => (
          <div className="turn" key={i}>
            <p className="q">{t.question}</p>
            {t.error && <p className="error">{t.error}</p>}
            {t.answer && !t.answer.done && <p className="muted">Genie is working… ({t.answer.status.toLowerCase().replace(/_/g, " ")})</p>}
            {t.answer?.done && (
              <>
                {t.answer.error && <p className="error">{t.answer.error}</p>}
                {t.answer.text && <p>{t.answer.text}</p>}
                {t.answer.sql && <details><summary>SQL</summary><pre>{t.answer.sql}</pre></details>}
                {t.answer.rows.length > 0 && (
                  <table className="table">
                    <thead><tr>{t.answer.columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                    <tbody>{t.answer.rows.map((r, j) => <tr key={j}>{r.map((v, k) => <td key={k}>{v}</td>)}</tr>)}</tbody>
                  </table>
                )}
              </>
            )}
          </div>
        ))}
      </div>
      <footer>
        <input value={question} placeholder="Ask about ED crowding…" onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && send()} disabled={busy} />
        <button className="primary" onClick={send} disabled={busy}>Ask</button>
      </footer>
    </aside>
  );
}
