// @ts-nocheck
import React from "react";

const ProjectInit = () => {
  return (
    <div className="min-h-screen bg-bg-base p-6 font-sans flex flex-col items-center">
      <div className="w-full max-w-4xl flex flex-col gap-8">
        
        {/* HERO SECTION */}
        <div className="flex flex-col items-center text-center mt-4">
          <h1 className="text-[15px] font-semibold text-text-primary mb-3">
            Welcome to Project Initiation and Source Selection
          </h1>
          <p className="text-text-secondary text-[11px] leading-relaxed max-w-2xl mb-10">
            Begin your Databricks data engineering workflow. Define your pipeline objectives, select your data source, and receive AI-powered recommendations for your pipeline architecture using Databricks best practices.
          </p>

          {/* STEPS */}
          <div className="flex justify-center gap-10 w-full mb-10">
            <div className="flex-1 max-w-[220px] text-center group">
              <div className="w-8 h-8 rounded-full bg-accent-blue text-white flex items-center justify-center mx-auto mb-3 text-[12px] font-bold shadow-sm transition-transform group-hover:scale-110 group-hover:shadow-[0_0_12px_rgba(59,130,246,0.5)]">
                1
              </div>
              <h3 className="text-[12px] font-semibold text-text-primary mb-1.5 transition-colors group-hover:text-accent-blue">Define Project Goals</h3>
              <p className="text-text-tertiary text-[11px] leading-relaxed">
                Describe your pipeline objectives in natural language
              </p>
            </div>

            <div className="flex-1 max-w-[220px] text-center group">
              <div className="w-8 h-8 rounded-full bg-accent-blue text-white flex items-center justify-center mx-auto mb-3 text-[12px] font-bold shadow-sm transition-transform group-hover:scale-110 group-hover:shadow-[0_0_12px_rgba(59,130,246,0.5)]">
                2
              </div>
              <h3 className="text-[12px] font-semibold text-text-primary mb-1.5 transition-colors group-hover:text-accent-blue">Select Data Source</h3>
              <p className="text-text-tertiary text-[11px] leading-relaxed">
                Choose from S3, JDBC databases, Unity Catalog, file uploads, or API endpoints
              </p>
            </div>

            <div className="flex-1 max-w-[220px] text-center group">
              <div className="w-8 h-8 rounded-full bg-accent-blue text-white flex items-center justify-center mx-auto mb-3 text-[12px] font-bold shadow-sm transition-transform group-hover:scale-110 group-hover:shadow-[0_0_12px_rgba(59,130,246,0.5)]">
                3
              </div>
              <h3 className="text-[12px] font-semibold text-text-primary mb-1.5 transition-colors group-hover:text-accent-blue">AI-Powered Planning</h3>
              <p className="text-text-tertiary text-[11px] leading-relaxed">
                Get intelligent pipeline recommendations and confirm your approach
              </p>
            </div>
          </div>

          {/* BUTTON */}
          <button className="bg-accent-blue text-white border-transparent rounded-lg px-4 py-2.5 text-[11px] font-medium flex items-center justify-center hover:bg-blue-600 transition-all shadow-sm hover:shadow-md hover:-translate-y-0.5 focus:ring-2 focus:ring-accent-blue focus:ring-offset-2 focus:ring-offset-bg-base">
            + Start New Project
          </button>
        </div>

        {/* WORKFLOW CONTEXT SECTION */}
        <div className="bg-bg-card border border-bg-border rounded-xl p-6 transition-all duration-300 hover:shadow-card hover:border-accent-blue/30 w-full mt-4">
          <div className="mb-6">
            <h2 className="text-[13px] font-semibold text-text-primary mb-1.5">Data Engineer Workflow Context</h2>
            <p className="text-text-secondary text-[11px] leading-relaxed">
              As a Data Engineer, you are leading the Databricks pipeline design and initiation process. This step establishes the foundation for the entire workflow.
            </p>
          </div>

          <div className="flex gap-10">
            {/* LEFT */}
            <div className="flex-1">
              <h3 className="text-text-tertiary text-[10px] font-semibold tracking-widest uppercase mb-4">Your Responsibilities</h3>
              <ul className="flex flex-col gap-3 m-0 p-0 list-none">
                {[
                  "Define clear project objectives and scope",
                  "Configure and validate data source connections",
                  "Review AI-generated pipeline recommendations",
                  "Approve the initial pipeline architecture",
                  "Leverage Unity Catalog for governance controls"
                ].map((item, idx) => (
                  <li key={idx} className="flex items-start gap-2.5 text-[11px] text-text-secondary leading-relaxed transition-colors hover:text-text-primary group">
                    <div className="w-1.5 h-1.5 rounded-full bg-accent-blue shrink-0 mt-1.5 shadow-[0_0_8px_rgba(59,130,246,0.5)] transition-transform group-hover:scale-125" />
                    <span>{item}</span>
                  </li>
                ))}
              </ul>
            </div>

            {/* RIGHT */}
            <div className="flex-1">
              <h3 className="text-text-tertiary text-[10px] font-semibold tracking-widest uppercase mb-4">Workflow Output</h3>
              <ul className="flex flex-col gap-3 m-0 p-0 list-none">
                {[
                  "Draft pipeline workspace with initial structure",
                  "Registered data source with configured connectors",
                  "AI-recommended sequence of pipeline steps",
                  "Foundation ready for transformation logic design",
                  "Collaboration setup for QA, AI Engineer, and DevOps"
                ].map((item, idx) => (
                  <li key={idx} className="flex items-start gap-2.5 text-[11px] text-text-secondary leading-relaxed transition-colors hover:text-text-primary group">
                    <div className="w-1.5 h-1.5 rounded-full bg-accent-green shrink-0 mt-1.5 shadow-[0_0_8px_rgba(16,185,129,0.5)] transition-transform group-hover:scale-125" />
                    <span>{item}</span>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
};

export default ProjectInit;