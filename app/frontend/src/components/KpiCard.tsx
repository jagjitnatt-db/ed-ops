import type { ApexOptions } from "apexcharts";
import type { ReactNode } from "react";
import Chart from "react-apexcharts";
import { BAR_GREY } from "../lib/cockpit";

/** Cockpit KPI card: coloured left edge, big figures on the left, grey trend bars on the right. */
export default function KpiCard({ accent, children, bars, barName, format, timeFormat }: {
  accent: string; children: ReactNode; bars: [number, number | null][]; barName: string;
  format: (v: number) => string; timeFormat: string;
}) {
  const options: ApexOptions = {
    chart: { type: "bar", toolbar: { show: false }, animations: { enabled: false } },
    colors: [BAR_GREY],
    plotOptions: { bar: { columnWidth: "70%", borderRadius: 0 } },
    dataLabels: { enabled: false },
    grid: { show: false },
    xaxis: { type: "datetime", labels: { datetimeUTC: false, format: timeFormat, style: { colors: "#6b7280" } },
      axisBorder: { color: "#d1d5db" }, axisTicks: { show: false } },
    yaxis: { labels: { formatter: format, style: { colors: "#6b7280" } }, tickAmount: 4 },
    tooltip: { x: { format: "dd MMM HH:mm" }, y: { formatter: format } },
  };
  return (
    <section className="kpi-card" style={{ borderLeftColor: accent }}>
      <div className="kpi-figures">{children}</div>
      <div className="kpi-chart">
        {bars.length === 0 ? <p className="muted">No history yet.</p> :
          <Chart type="bar" height={230} options={options} series={[{ name: barName, data: bars }]} />}
      </div>
    </section>
  );
}
