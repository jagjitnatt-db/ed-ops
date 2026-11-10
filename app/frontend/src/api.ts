import { useEffect, useState } from "react";
import type { Band } from "./lib/bands";

export type Driver = { driver: string; value: number | null; baseline: number | null; z: number | null };
export type Facility = {
  facility_id: string; facility_name: string; facility_type: string; region: string; snapshot_ts: string;
  census: number; gedwin: number | null; gedwin_band: Band; gedwin_delta_1h: number | null; edwin: number | null;
  edwin_band: Band | null; nedocs: number; nedocs_level: number; occupancy_ratio: number | null;
  d2p_mean_min: number | null; d2p_median_min: number | null; d2p_p90_min: number | null; d2p_sample_n: number;
  waiting_now_n: number; longest_current_wait_min: number | null; boarders_n: number; longest_boarder_h: number | null;
  lwbs_rate_24h: number | null; los_median_h_24h: number | null; attendings_on_duty: number | null;
  patients_per_attending: number | null; arrival_surge_ratio: number | null; undertriage_flags_n: number;
  top_drivers: Driver[] | null; computed_at: string; sparkline?: [string, number | null][];
  ed_beds?: number; inpatient_beds?: number;
};
export type Narrative = { headline: string; explanation: string; recommended_actions: string[] | null; top_drivers?: Driver[] | null;
  crowded_facilities?: string[] | null; doing_well_facilities?: string[] | null; generated_at?: string };
export type Summary = { counts: Record<Band, number>; facilities: number; census: number; boarders: number;
  network_d2p_median_min: number | null; as_of: string | null; narrative: Narrative | null };
export type Health = { status: "ok" | "delayed" | "unavailable"; data_age_seconds: number | null };
export type Point = { snapshot_ts: string; gedwin: number | null; edwin: number | null; nedocs: number | null;
  d2p_median_min: number | null; d2p_p90_min: number | null; d2p_sample_n: number; census: number;
  n_esi1: number; n_esi2: number; n_esi3: number; n_esi4: number; n_esi5: number; n_untriaged: number;
  boarders_n: number; waiting_now_n: number; longest_current_wait_min: number | null; lwbs_rate_24h: number | null };
export type NetworkBucket = { bucket: string; census: number; d2p_median_min: number | null; waiting_now_n: number;
  longest_current_wait_min: number | null; boarders_n: number; lwbs_rate_24h: number | null; crowded_n: number };
export type Patient = { visit_id: string; status: string; esi_level: number | null; suggested_esi: number | null;
  undertriage_flag: boolean | null; complaint_category: string | null; minutes_in_ed: number; minutes_waiting: number;
  arrival_mode: string; on_vent: boolean };
export type Cell = { facility_id: string; facility_name: string; hour: string; gedwin: number | null; band: Band };
export type GenieAnswer = { done: boolean; status: string; text: string | null; sql: string | null; columns: string[];
  rows: string[][]; error: string | null };
export type Definitions = { formulas: Record<string, string>; assumptions: string[]; citations: string[];
  bands: { gedwin: { busy: number; crowded: number }; edwin: { busy: number; crowded: number } } };

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, init);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(body.detail ?? `${r.status} ${r.statusText}`);
  }
  return r.json() as Promise<T>;
}

export const api = {
  health: () => request<Health>("/api/health"),
  summary: () => request<Summary>("/api/network/summary"),
  facilities: () => request<{ facilities: Facility[] }>("/api/facilities"),
  facility: (id: string) => request<{ facility: Facility; narrative: Narrative | null }>(`/api/facilities/${id}`),
  history: (id: string, hours: number) => request<{ points: Point[] }>(`/api/facilities/${id}/history?hours=${hours}`),
  board: (id: string) => request<{ patients: Patient[] }>(`/api/facilities/${id}/board`),
  heatmap: () => request<{ cells: Cell[] }>("/api/heatmap"),
  networkHistory: (hours: number) => request<{ bucket_minutes: number; buckets: NetworkBucket[] }>(`/api/network/history?hours=${hours}`),
  definitions: () => request<Definitions>("/api/definitions"),
  ask: (question: string, conversation_id?: string) =>
    request<{ conversation_id: string; message_id: string }>("/api/genie/messages", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question, conversation_id }),
    }),
  poll: (c: string, m: string) => request<GenieAnswer>(`/api/genie/conversations/${c}/messages/${m}`),
};

/** Fetch now and every `ms` milliseconds (the data refreshes about once a minute; 30 s polling is enough). */
export function usePolling<T>(fn: () => Promise<T>, ms: number, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const load = () =>
      fn().then((d) => alive && (setData(d), setError(null))).catch((e: Error) => alive && setError(e.message));
    load();
    const id = setInterval(load, ms);
    return () => { alive = false; clearInterval(id); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return { data, error };
}
