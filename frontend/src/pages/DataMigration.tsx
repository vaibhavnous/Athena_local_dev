// @ts-nochecks
import React, { useState } from "react";

const MigrationSetup = () => {
  const [sourceDatabase, setSourceDatabase] = useState("");
  const [targetDatabase, setTargetDatabase] = useState("");

  const ChevronDownIcon = () => (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );

  const ArrowRightIcon = () => (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="5" y1="12" x2="19" y2="12"></line>
      <polyline points="12 5 19 12 12 19"></polyline>
    </svg>
  );

  return (
    <div className="min-h-screen bg-bg-base p-6 font-sans flex justify-center">
      <div className="w-full max-w-5xl flex flex-col gap-6">
        
        {/* STEPS */}
        <div className="flex items-center w-full mb-6 mt-2">
          
          {/* STEP 1 */}
          <div className="flex items-center gap-3 cursor-default group">
            <div className="w-8 h-8 rounded-full border border-accent-blue bg-accent-blue/10 flex items-center justify-center text-accent-blue text-[12px] font-bold shadow-[0_0_8px_rgba(59,130,246,0.2)] transition-transform group-hover:scale-110">1</div>
            <div className="flex flex-col">
              <span className="text-[13px] font-semibold text-text-primary transition-colors group-hover:text-accent-blue">Select Source &amp; Database</span>
              <span className="text-[11px] text-text-tertiary mt-0.5">Choose databases for migration</span>
            </div>
          </div>

          <div className="flex-1 h-px bg-bg-border mx-6"></div>

          {/* STEP 2 */}
          <div className="flex items-center gap-3 cursor-default group">
            <div className="w-8 h-8 rounded-full border border-bg-border bg-bg-card flex items-center justify-center text-text-tertiary text-[12px] font-bold transition-colors group-hover:border-text-secondary group-hover:text-text-secondary">2</div>
            <div className="flex flex-col">
              <span className="text-[13px] font-semibold text-text-secondary transition-colors group-hover:text-text-primary">Configure Migration</span>
              <span className="text-[11px] text-text-tertiary mt-0.5">Select tables and strategy</span>
            </div>
          </div>

          <div className="flex-1 h-px bg-bg-border mx-6"></div>

          {/* STEP 3 */}
          <div className="flex items-center gap-3 cursor-default group">
            <div className="w-8 h-8 rounded-full border border-bg-border bg-bg-card flex items-center justify-center text-text-tertiary text-[12px] font-bold transition-colors group-hover:border-text-secondary group-hover:text-text-secondary">3</div>
            <div className="flex flex-col">
              <span className="text-[13px] font-semibold text-text-secondary transition-colors group-hover:text-text-primary">Results &amp; Script</span>
              <span className="text-[11px] text-text-tertiary mt-0.5">Review and edit migration script</span>
            </div>
          </div>
        </div>

        {/* CONTAINER FOR CONTENT */}
        <div className="bg-bg-card border border-bg-border rounded-xl p-6 transition-all duration-300 hover:shadow-card hover:border-accent-blue/30 w-full mb-2">
          {/* FORM */}
          <div className="flex gap-8 mb-6">
            
            <div className="flex-1 flex flex-col gap-2.5">
              <label className="text-[10px] font-semibold text-text-primary uppercase tracking-widest ml-1">Select Source Database</label>
              <div className="relative group">
                <select 
                  className="w-full bg-bg-base border border-bg-border rounded-lg px-3.5 py-2.5 text-[12px] text-text-secondary focus:outline-none focus:ring-1 focus:ring-accent-blue focus:border-accent-blue transition-all appearance-none cursor-pointer group-hover:border-bg-border/80 group-hover:bg-bg-hover/30"
                  value={sourceDatabase}
                  onChange={(e) => setSourceDatabase(e.target.value)}
                >
                  <option value="" disabled>Select source database...</option>
                  <option value="production-db">Production DB</option>
                  <option value="postgresql">PostgreSQL</option>
                </select>
                <div className="absolute right-3.5 top-1/2 -translate-y-1/2 text-text-tertiary pointer-events-none group-hover:text-text-secondary transition-colors">
                  <ChevronDownIcon />
                </div>
              </div>
            </div>

            <div className="flex-1 flex flex-col gap-2.5">
              <label className="text-[10px] font-semibold text-text-primary uppercase tracking-widest ml-1">Select Target Database</label>
              <div className="relative group">
                <select 
                  className="w-full bg-bg-base border border-bg-border rounded-lg px-3.5 py-2.5 text-[12px] text-text-secondary focus:outline-none focus:ring-1 focus:ring-accent-blue focus:border-accent-blue transition-all appearance-none cursor-pointer group-hover:border-bg-border/80 group-hover:bg-bg-hover/30"
                  value={targetDatabase}
                  onChange={(e) => setTargetDatabase(e.target.value)}
                >
                  <option value="" disabled>Select target database...</option>
                  <option value="analytics-cluster">Analytics cluster</option>
                  <option value="postgresql">PostgreSQL</option>
                </select>
                <div className="absolute right-3.5 top-1/2 -translate-y-1/2 text-text-tertiary pointer-events-none group-hover:text-text-secondary transition-colors">
                  <ChevronDownIcon />
                </div>
              </div>
            </div>

          </div>

          {/* BUTTON */}
          <div className="flex flex-col items-start gap-3 mt-4">
            <button className="bg-accent-blue border-transparent text-white rounded-lg px-4 py-2 text-[11px] font-medium inline-flex items-center justify-center gap-1.5 hover:bg-blue-600 transition-all shadow-sm hover:shadow-md hover:-translate-y-0.5 focus:ring-2 focus:ring-accent-blue focus:ring-offset-2 focus:ring-offset-bg-card">
              <span>+ AI Source Discovery</span>
              <ArrowRightIcon />
            </button>

            <span className="text-[11px] text-text-tertiary leading-relaxed max-w-lg">
              AI will analyze your source database structure. This process runs every 30 seconds and may take several minutes.
            </span>
          </div>
        </div>

        {/* LOGS */}
        <div className="flex flex-col">
          <div className="flex justify-between items-center mb-3 px-1">
            <h3 className="text-[13px] font-semibold text-text-primary m-0">Migration Logs</h3>
            <span className="text-[11px] text-text-tertiary font-medium">No analysis yet</span>
          </div>

          <div className="bg-[#050505] border border-bg-border rounded-xl p-4 h-[240px] text-[11px] text-gray-400 font-mono leading-relaxed shadow-inner overflow-auto w-full">
            <div className="mb-2 opacity-50"># Waiting for configuration...</div>
            <div className="text-gray-300">
              Select source and databases, then click <span className="text-accent-blue">"AI Source Discovery"</span> to begin analysis.
            </div>
          </div>
        </div>

      </div>
    </div>
  );
};

export default MigrationSetup;