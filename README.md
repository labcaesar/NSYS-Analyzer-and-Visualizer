# NSYS Analyzer & Visualizer (NAV)

## Introduction  
High-performance computing (HPC) and AI workloads increasingly rely on **GPUs** for acceleration, yet **understanding performance impacts** of code changes remains challenging. **NVIDIA Nsight™ Systems (NSYS)** provides profiling tools, but lacks **efficient comparative analysis and detailed visualization** capabilities.  

**NAV (NSYS Analyzer and Visualizer)** enhances NSYS by offering **fast, automated, and insightful trace analysis**, helping developers and researchers quickly identify **performance regressions, bottlenecks, and optimizations** in GPU workloads.  

## Key Features  
✔ **Faster Extraction & Visualization** – Extracts trace data significantly faster (compared to NSYS recipes), **23.33× speedup** for 1.2G traces, **9.75× speedup** for 12G traces  
✔ **Comparative Analysis** – Enables **direct side-by-side performance comparisons** of multiple traces  
✔ **Advanced Data Representations** – Generates **histograms, violin plots, and multi-trace visualizations**  
✔ **Multi-Level Granularity** – Supports **Micro, Meso, and Macro-level** insights for deeper analysis  
✔ **Efficient Handling of Large Traces** – Uses **parallel processing** to manage high-frequency GPU traces  
✔ **Multiple Export Formats** – Save results in **CSV, LaTeX, and PNG** for easy reporting and integration  
✔ **Open-Source & Extensible** – Modify and extend NAV to **add new metrics, visualizations, and analyses** 

## Why Use NAV?  
🔹 **Automates** performance trace analysis, reducing manual effort  
🔹 **Uncovers hidden performance trends** that NSYS recipes may miss  
🔹 **Improves regression testing** by providing **intuitive, side-by-side comparisons**  
🔹 **Optimized for HPC, AI/ML, and GPU-intensive applications**  

---

## Requirements  

### Required Trace Flags  
Run NSYS with the necessary flags for full trace capture:  
```bash
nsys profile --trace=cuda,mpi,ucx,nvtx 
```

### Extracting SQLite File from NSYS Report  
Convert an `.nsys-rep` file to an `.sqlite` database for NAV:  
```bash
nsys export --type sqlite <nsys.rep file>
```
Alternatively, opening the `.nsys-rep` file in the Nsight GUI may automatically generate an `.sqlite` file.  

### Required Python Libraries  
Ensure your environment has all dependencies installed:  
```bash
pip install absl_py contourpy cycler fonttools joblib kiwisolver \
matplotlib numpy packaging pillow pyparsing python_dateutil \
scikit_learn scipy six sklearn threadpoolctl
```

### **Pre-Compile Scripts for Faster Execution**  
Before running NAV, you can **precompile** the Python scripts to speed up future executions:  
```bash
python -m compileall .
```
---

## Script Usage  

### Generating NAV JSON Files from SQLite  
Extract data and generate tables/figures from an `.sqlite` trace file:  
```bash
python3 main.py -df file.sqlite
```
Extract data **without** generating tables/figures (useful for batch processing):  
```bash
python3 main.py -df file.sqlite -nmo
```
Extract data from multiple `.sqlite` files sequentially (**not recommended due to slow performance**):  
```bash
python3 main.py -df "file1.sqlite file2.sqlite file3.sqlite" -mdl "Label1,Label2,Label3"
```

### Recommended: Parallel Extraction for Multiple SQLite Files  
Run extractions separately to speed up processing:  
```bash
# Execute on separate nodes or jobs in parallel
python3 main.py -df "file1.sqlite" -nmo &
python3 main.py -df "file2.sqlite" -nmo &
python3 main.py -df "file3.sqlite" -nmo &
```

### Generating Tables and Figures from NAV Files  
Process a single NAV file:  
```bash
python3 main.py -jf file.nav
```
Process multiple NAV files with comparative analysis:  
```bash
python3 main.py -jf "file1.nav file2.nav file3.nav" -mdl "Label1,Label2,Label3"
```

---

## Flags Overview  

### General Flags  
- `-o, --output_dir` → Output directory for NAV files, tables, and figures *(default: ./output)*  
- `-mdl, --multi_data_label` → *(Required for multi-file analysis)* Labels for each trace *(e.g., "1 GPU, 2 GPU, 3 GPU")*  
- `-mw, --max_workers` → Number of threads to use *(Defaults to CPU count if unset)*  

### Extraction Flags  
- `-df, --data_file` → Specify an `.sqlite` trace file for extraction  
- `-nf, --nav_file` → Use an existing NAV `.nav` file instead of extracting from `.sqlite`  
- `-nkm, --no_kernel_metrics` → Skip exporting kernel metrics  
- `-ntm, --no_transfer_metrics` → Skip exporting transfer metrics  
- `-ncm, --no_communication_metrics` → Skip exporting communication metrics  
- `-nsd, --no_save_data` → Prevent saving extracted data to a NAV file  

### Graphics & Table Flags  
- `-nmo, --no_metrics_output` → Disable metrics export after extraction  
- `-ncmo, --no_compare_metrics_output` → Disable comparison metric exports (for multi-file analysis)  
- `-ngmo, --no_general_metrics_output` → Disable general metric exports (Kernel, Transfer, Communication)  
- `-nsmo, --no_specific_metrics_output` → Disable specific metric exports (Duration, Size, Slack, Overhead, etc.)  
- `-nimo, --no_individual_metrics_output` → Disable exporting individual metric details  

---
