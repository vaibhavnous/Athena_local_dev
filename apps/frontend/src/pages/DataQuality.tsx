// @ts-nochecks
import React from "react";
import { Shield } from "lucide-react";

const DataQualityMonitoring = () => {
  const ChevronDownIcon = () => (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );

  const SettingsIcon = () => (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="3"></circle>
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path>
    </svg>
  );

  const metrics = [
    { label: "Completeness", value: "97%", color: "bg-accent-green", width: "97%" },
    { label: "Accuracy", value: "94%", color: "bg-accent-green", width: "94%" },
    { label: "Consistency", value: "89%", color: "bg-yellow-500", width: "89%" },
    { label: "Timeliness", value: "99%", color: "bg-accent-green", width: "99%" },
  ];

  return (
    <div className="min-h-screen bg-bg-base p-6 font-sans flex justify-center">
      <div className="w-full max-w-6xl">
        
        {/* MAIN CARD */}
        <div className="bg-bg-card border border-bg-border rounded-xl p-6 shadow-sm transition-all duration-300 hover:shadow-card hover:border-accent-blue/30 w-full mb-6">
          
          {/* HEADER */}
          <div className="flex justify-between items-center mb-5">
            <h1 className="m-0 flex items-center gap-2 text-[15px] font-semibold text-text-primary">
              <Shield size={16} className="text-accent-blue" />
              Data Quality Monitoring
            </h1>

            <div className="flex gap-3">
              <button className="bg-transparent border border-accent-blue text-accent-blue rounded-lg px-3 py-1.5 text-[11px] font-medium inline-flex items-center gap-1.5 hover:bg-accent-blue/10 transition-colors focus:ring-2 focus:ring-accent-blue focus:ring-offset-2 focus:ring-offset-bg-card">
                <SettingsIcon />
                <span>AI Recommendations</span>
              </button>
              
              <div className="relative group">
                <select className="bg-bg-base border border-bg-border rounded-lg pl-3 pr-8 py-1.5 text-[11px] text-text-secondary focus:outline-none focus:ring-1 focus:ring-accent-blue transition-colors appearance-none cursor-pointer hover:bg-bg-hover/30">
                  <option>Last 24 hours</option>
                  <option>Last 7 days</option>
                  <option>Last 30 days</option>
                </select>
                <div className="absolute right-2.5 top-1/2 -translate-y-1/2 text-text-tertiary pointer-events-none transition-colors group-hover:text-text-secondary">
                  <ChevronDownIcon />
                </div>
              </div>
            </div>
          </div>

          <div className="h-px bg-bg-border w-full mb-6"></div>

          {/* GRID */}
          <div className="flex gap-10">
            
            {/* LEFT: METRICS */}
            <div className="flex-[1.1] flex flex-col">
              <h3 className="text-[10px] font-semibold tracking-widest uppercase text-text-primary mb-5">Quality Metrics</h3>

              <div className="flex flex-col gap-4">
                {metrics.map((m, idx) => (
                  <div key={idx} className="flex items-center justify-between group">
                    <span className="text-[11px] font-medium text-text-secondary w-20 truncate transition-colors group-hover:text-text-primary">{m.label}</span>
                    <div className="flex-[1.5] bg-bg-border h-1 rounded-full mx-4 overflow-hidden relative">
                      <div className={`absolute top-0 left-0 h-full rounded-full ${m.color} transition-all duration-1000 ease-out`} style={{ width: m.width }}></div>
                    </div>
                    <span className="text-[11px] font-bold text-text-primary w-8 text-right">{m.value}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* MIDDLE: RULES */}
            <div className="flex-[1.4] flex flex-col">
              <h3 className="text-[10px] font-semibold tracking-widest uppercase text-text-primary mb-5">Active Validation Rules</h3>

              <div className="flex flex-col gap-3.5">
                {[
                  { title: "Email Format", desc: "Validates email addresses using regex" },
                  { title: "Phone Number", desc: "Checks for valid phone format" },
                  { title: "Age Range", desc: "Validates age between 0-120" }
                ].map((rule, idx) => (
                  <div key={idx} className="bg-bg-base border border-bg-border rounded-xl p-3.5 relative transition-all hover:border-bg-border hover:shadow-sm group">
                    <div className="w-1.5 h-1.5 rounded-full bg-accent-green absolute right-3.5 top-4 shadow-[0_0_8px_rgba(16,185,129,0.5)] transition-transform group-hover:scale-125"></div>
                    <div className="text-[12px] font-semibold text-text-primary mb-1">{rule.title}</div>
                    <p className="text-[10px] text-text-tertiary m-0 leading-relaxed max-w-[90%]">{rule.desc}</p>
                  </div>
                ))}
              </div>
            </div>

            {/* RIGHT: AI */}
            <div className="flex-[1.4] flex flex-col">
              <h3 className="text-[10px] font-semibold tracking-widest uppercase text-text-primary mb-5">AI Recommendations</h3>

              <div className="flex flex-col gap-3.5">
                
                <div className="bg-blue-500/5 border border-blue-500/30 rounded-xl p-3.5 transition-all hover:border-blue-500/50 hover:bg-blue-500/10">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-[11px]">💡</span>
                    <strong className="text-[12px] font-semibold text-blue-400">Optimize Schema</strong>
                  </div>
                  <p className="text-[10px] text-text-tertiary m-0 leading-relaxed ml-5">
                    Consider adding constraints to improve performance
                  </p>
                </div>

                <div className="bg-yellow-500/5 border border-yellow-500/30 rounded-xl p-3.5 transition-all hover:border-yellow-500/50 hover:bg-yellow-500/10">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-[11px]">⚠</span>
                    <strong className="text-[12px] font-semibold text-yellow-500">Data Drift Detected</strong>
                  </div>
                  <p className="text-[10px] text-text-tertiary m-0 leading-relaxed ml-5">
                    Customer age distribution has shifted
                  </p>
                </div>

                <div className="bg-green-500/5 border border-green-500/30 rounded-xl p-3.5 transition-all hover:border-green-500/50 hover:bg-green-500/10">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-[11px]">✔</span>
                    <strong className="text-[12px] font-semibold text-accent-green">Quality Improved</strong>
                  </div>
                  <p className="text-[10px] text-text-tertiary m-0 leading-relaxed ml-5">
                    Recent changes increased data quality by 3%
                  </p>
                </div>

              </div>
            </div>

          </div>
        </div>
      </div>
    </div>
  );
};

export default DataQualityMonitoring;
