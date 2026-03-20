# IDH Mutation Prediction from Brain Tumour Whole Slide Images

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

> End-to-end computational pathology pipeline that predicts **IDH mutation status** (mIDH+ vs mIDH&minus;) from digitised TCGA glioma biopsy slides using Gated Attention Multiple Instance Learning.

<p align="center">
  <img src="reports/project_internal_img/fe892e8f-652e-489b-99d9-a2925d7bc14c.jpg" width="900" alt="Computational pathology: from histology to mutation prediction" />
</p>

---

## Table of Contents

- [Overview](#overview)
- [Key Results](#key-results)
- [Pipeline](#pipeline)
- [Visual Results](#visual-results)
- [Where to Start](#where-to-start)
- [Repository Structure](#repository-structure)
- [Architecture](#architecture)
- [Running the Project](#running-the-project)
- [Reproducibility & Notes](#reproducibility--notes)
- [Glossary](#glossary)
- [Acknowledgements](#acknowledgements)

---

## Overview

This project implements a complete machine learning pipeline for classifying **IDH mutation status** from Whole Slide Images (WSI) of brain tumours.

The dataset is derived from The Cancer Genome Atlas (TCGA) glioma cohort and includes 100 labelled training slides and 25 unlabelled test slides.

Since each WSI can reach several gigabytes in size, the pipeline processes the data through four stages:

- Tissue segmentation
- Patch extraction
- Foundation model embedding
- Attention-based classification

These stages produce a single prediction per slide.

The main classifier is a **Gated Attention Multiple Instance Learning** (ABMIL) model that achieves an **AUC of 0.994** using 5-fold cross-validation.

Attention and contribution heatmaps provide qualitative interpretability by highlighting the tissue regions that most strongly influence each prediction.

<p align="center">
  <img src="reports/project_internal_img/background-video-fe892e8f-652e-489b-99d9-a2925d7bc14c.gif" width="800" alt="From histology slide to mutation prediction" />
</p>
<p align="center"><em>From histology slide to mutation prediction</em></p>

---

## Key Results

All metrics are 5-fold GroupKFold CV (grouped by patient ID), threshold fixed at 0.5.

| Model | AUC | F1 | Sensitivity | Specificity |
|:------|:---:|:--:|:-----------:|:-----------:|
| **Gated ABMIL** | **0.994 &pm; 0.005** | **0.937 &pm; 0.028** | **0.956 &pm; 0.058** | **0.918 &pm; 0.070** |
| Baseline (LogReg + mean-pool) | 0.950 &pm; 0.051 | 0.879 &pm; 0.074 | 0.892 &pm; 0.107 | 0.883 &pm; 0.071 |

**Test set predictions:** 25 slides &rarr; 13 mIDH+ / 12 mIDH&minus; (ensemble of 5 fold checkpoints).

---

## Pipeline

<p align="center">
  <img src="references/pipeline-overview/pipeline-phases1-4-flow.png" width="900" alt="End-to-end pipeline data flow (Phases 1-4)" />
</p>
<p align="center"><em>End-to-end data flow from raw SVS slides to UNI2-h embeddings (Phases 1&ndash;4)</em></p>

| Phase | Stage | Input | Output | Key Tool / Model |
|:-----:|:------|:------|:-------|:-----------------|
| 1 | **Configuration & Data Ingestion** | `config.yaml`, CSVs | Indexed DataFrames with `case_id`, `svs_path` | `DataConfigManager`, `DataIngestor` |
| 2 | **Tissue Segmentation** | `.svs` WSI files | GeoJSON tissue polygons + thumbnails | GrandQC via Trident `Processor` |
| 3 | **Patch Extraction** | GeoJSON polygons + SVS | H5 coordinate files (256&times;256 px at 20&times;) | Trident `Processor` |
| 4 | **Feature Extraction** | Patch coordinates + SVS | 1536-D embedding vectors per patch (H5) | UNI2-h foundation model |
| 5 | **Classification** | Patch embeddings (H5) | Fold checkpoints + per-fold metrics | Gated Attention MIL (ABMIL) |
| 6 | **Inference** | Test embeddings + checkpoints | Submission CSV (slide_id, pred) | 5-fold ensemble (avg. probabilities) |
| 7 | **Explainability** | Attention weights + embeddings | Spatial heatmaps (attention & contribution) | `AttentionVisualizer` |

---

## Visual Results

The figures below span four aspects of the pipeline: spatial interpretability via attention and contribution heatmaps, tissue coverage statistics, embedding geometry, and the raw patch content the model operates on.

<table>
<tr>
<td align="center"><strong>Attention heatmap</strong> (mIDH+ slide, P=0.980)</td>
<td align="center"><strong>Contribution heatmap</strong> (mIDH+ slide, P=0.980)</td>
</tr>
<tr>
<td><img src="reports/figures/phase7-heatmaps/val_attention_pos.png" width="450" alt="Attention heatmap — mIDH+ slide" /></td>
<td><img src="reports/figures/phase7-heatmaps/val_contribution_pos.png" width="450" alt="Contribution heatmap — mIDH+ slide" /></td>
</tr>
<tr>
<td align="center"><strong>Patch count by IDH status</strong> (train set, n=100)</td>
<td align="center"><strong>t-SNE of slide-level embeddings</strong> (UNI2-h, n=100)</td>
</tr>
<tr>
<td><img src="reports/figures/phase3-patches/patch_count_distribution.png" width="450" alt="Boxplot: patch count per slide by IDH status" /></td>
<td><img src="reports/figures/phase_4/tsne_q7_p30.png" width="450" alt="t-SNE of mean-pooled UNI2-h embeddings coloured by mIDH status" /></td>
</tr>
</table>

**Top-10 highest-attention patches** from a confirmed mIDH+ validation slide (P=0.980):

<p align="center">
  <img src="reports/figures/phase7-heatmaps/val_top10_pos/top_10_grid.png" width="900" alt="Top-10 attended patches — mIDH+ slide" />
</p>

- **Attention heatmap** (inferno colormap): shows *where* the model looked &mdash; patches with the highest learned attention weights.
- **Contribution heatmap** (red-blue diverging): shows *how* each patch influenced the prediction &mdash; red patches push toward mIDH+, blue patches push toward mIDH&minus;. Computed as `attention_weight * dot(classifier_weight, patch_embedding)`.
- Resolution is at the **patch level** (256&times;256 px at 20&times; magnification). Heatmaps are overlaid on slide thumbnails at 2048 px width.
- Opacity scales with signal strength so low-signal regions are nearly transparent.
- These heatmaps are **qualitative** &mdash; they highlight morphological regions associated with the prediction but do not prove biological causation.
- Top-K attended patches are exported as grids with coordinates for further inspection (see `reports/figures/phase7-heatmaps/`).

---

## Where to Start

> **Note on notebook outputs:** The master submission notebook's cell outputs were cleared to stay under GitHub's 100 MB file limit. All figures are preserved as PNGs in [`reports/figures/`](reports/figures/). Use the nbviewer links below to view rendered outputs for the analysis notebooks.

| What | Source | View with outputs |
|:-----|:-------|:-----------------|
| **Master submission notebook** (Q1&ndash;Q11) | [`notebooks/IDH-Mutation-Prediction-Pipeline.ipynb`](notebooks/IDH-Mutation-Prediction-Pipeline.ipynb) | [![nbviewer](https://img.shields.io/badge/render-nbviewer-orange?logo=jupyter)](https://nbviewer.org/github/Danielevko/idh-mutation-prediction/blob/main/notebooks/IDH-Mutation-Prediction-Pipeline.ipynb) |
| **Final submission CSV** | [`data/predictions/abmil/2026-03-03_22-08-08_submission.csv`](data/predictions/abmil/2026-03-03_22-08-08_submission.csv) | — |
| Saved figures &mdash; all phases | [`reports/figures/`](reports/figures/) | — |
| Original assignment PDF | [`references/Computational Pathology Hello World (Kaggle).pdf`](references/Computational%20Pathology%20Hello%20World%20(Kaggle).pdf) | — |

---

## Repository Structure

```
idh_env/                                    # Project root
│
├── configs/
│   ├── config.yaml                         # All data paths + HuggingFace token — NOT tracked (secrets)
│   ├── config.yaml.example                 # Template — copy to config.yaml and fill in your values
│   └── models.yaml                         # Model hyperparameters (training, baseline, ABMIL)
│
├── idh_env/                                # Python package (all pipeline logic)
│   ├── __init__.py                         # Re-exports all public classes
│   ├── config.py                           # DataConfigManager — single entry point for paths & secrets
│   ├── data_ingestor.py                    # DataIngestor — reads CSVs, adds case_id + svs_path
│   ├── tissue_segmentor.py                 # TissueSegmentor — GrandQC segmentation via Trident
│   ├── segmentation_debugger.py            # SegmentationDebugger — visual QC overlays
│   ├── patch_extractor.py                  # PatchExtractor — 256×256 patch coordinate extraction
│   ├── patch_debugger.py                   # PatchDebugger — patch visual QC
│   ├── exploratory_analyzer.py             # ExploratoryAnalyzer — patch count EDA + stats
│   ├── feature_extractor.py               # FeatureExtractor — UNI2-h embeddings via Trident
│   ├── embedding_analyzer.py              # EmbeddingAnalyzer — t-SNE batch-effect diagnostic
│   ├── attention_visualizer.py            # AttentionVisualizer — heatmaps (attention + contribution)
│   └── modeling/
│       ├── __init__.py                     # Exports Trainer, Predictor, BaselineModel, AttentionMIL
│       ├── trainer.py                      # Generic GroupKFold CV loop with metadata persistence
│       ├── predictor.py                    # Ensemble inference (averages fold probabilities)
│       ├── baseline_model.py              # BaselineModel — sklearn LogisticRegression wrapper
│       └── attention_mil.py               # AttentionMIL + MILDataset + mil_collate (Gated ABMIL)
│
├── notebooks/
│   ├── IDH-Mutation-Prediction-Pipeline.ipynb   # PRIMARY SUBMISSION — answers Q1–Q11
│   ├── 1-tissue-segmentation.ipynb              # Pipeline runner: segmentation + pen mark removal
│   ├── 2-patch-extraction.ipynb                 # Pipeline runner: patch coordinate extraction
│   ├── 3-feature-extraction-uni2h.ipynb         # Pipeline runner: UNI2-h embedding extraction
│   ├── 4-baseline-classifier-logreg.ipynb       # Training runner: baseline LogReg classifier
│   └── 5-abmil-attention-mil.ipynb              # Training runner: ABMIL + LGG subgroup + heatmaps
│
├── data/
│   ├── raw/
│   │   ├── train.csv                       # 100-slide training labels (tracked in git)
│   │   ├── test.csv                        # 25-slide test manifest (tracked in git)
│   │   ├── train/                          # 100 TCGA .svs files — NOT tracked (too large)
│   │   └── test/                           # 25 TCGA .svs files — NOT tracked (too large)
│   ├── interim/                            # Segmentation outputs (GeoJSON polygons + patch H5 coords) — NOT tracked
│   ├── processed/                          # UNI2-h feature embeddings (N, 1536) per slide — NOT tracked
│   └── predictions/                        # Submission CSVs (slide_id, pred) — tracked in git
│
├── models/                                 # Saved checkpoints (per-fold .pt/.pkl + metadata JSON)
│   ├── abmil/                              # ABMIL runs (5-fold checkpoints per timestamp)
│   └── baseline/                           # Baseline LogReg runs
│
├── reports/
│   └── figures/                            # All saved figures organised by pipeline phase
│
├── references/
│   ├── pipeline-overview/                  # End-to-end pipeline walkthrough + diagram
│   ├── architecture/                       # UML class diagram (drawio + png)
│   └── technical-notes/                    # Deep-dive docs (one per pipeline stage)
│
├── Makefile                                # lint, format, data, clean targets
└── pyproject.toml                          # Package metadata + ruff config (Python ≥3.12)
```

| Directory | Purpose | When to look here |
|:----------|:--------|:------------------|
| `notebooks/` | All Jupyter notebooks &mdash; pipeline runners + submission | Start with `IDH-Mutation-Prediction-Pipeline.ipynb` |
| `idh_env/` | Python package with all pipeline logic | Reading or modifying pipeline code |
| `data/raw/` | Immutable input data (SVS slides + label CSVs) | Setting up the dataset |
| `data/processed/embeddings/` | UNI2-h feature H5 files (1536-D per patch) | Retraining classifiers without re-extracting |
| `data/predictions/` | Final submission CSVs | Reviewing or submitting predictions |
| `models/` | Saved model checkpoints + per-fold metadata | Loading trained models for inference |
| `reports/figures/` | All saved figures by pipeline phase | Reviewing visual outputs |
| `references/technical-notes/` | Deep-dive technical documentation | Understanding design decisions |
| `configs/` | YAML configuration (paths, hyperparameters) | Adapting the pipeline to a new environment |

---

## Architecture

> Click the diagram to open the full-resolution version.

<p align="center">
  <a href="references/architecture/project_architecture.png" target="_blank">
    <img src="references/architecture/project_architecture.png" width="100%" alt="IDH Mutation Pipeline — UML Class Diagram" />
  </a>
</p>
<p align="center"><em>UML class diagram — all 12 pipeline modules, their attributes, methods, and dependencies. Click to zoom.</em></p>

---

## Running the Project

### Environment setup

```bash
# Create a Python 3.12 virtual environment
python3.12 -m venv ~/.venvs/medical_ml
source ~/.venvs/medical_ml/bin/activate

# Install the package in editable mode (includes core deps)
pip install -e .

# Additional dependencies (not in pyproject.toml)
pip install pandas torch torchvision openslide-python h5py scikit-learn \
            matplotlib seaborn huggingface_hub timm
```

### Expected data layout

Place the TCGA glioma dataset under `data/raw/`:

```
data/raw/
├── train.csv          # columns: slide_id, mIDH, ...
├── test.csv           # columns: slide_id, ...
├── train/             # 100 .svs files
└── test/              # 25 .svs files
```

Update absolute paths in `configs/config.yaml` to match your local file system. Add your HuggingFace token (required for UNI2-h weight download) under `model.huggingface_token`.

### Reproducing results

The master notebook [`notebooks/IDH-Mutation-Prediction-Pipeline.ipynb`](notebooks/IDH-Mutation-Prediction-Pipeline.ipynb) runs the full analysis. Heavy compute steps (segmentation, patching, feature extraction) are summarised with pre-computed outputs; all classification, analysis, and explainability sections run live code.

To run individual pipeline stages from scratch:

```bash
source ~/.venvs/medical_ml/bin/activate

# 1. Tissue segmentation (GrandQC)
jupyter nbconvert --execute notebooks/1-tissue-segmentation.ipynb

# 2. Patch extraction (256×256 at 20×)
jupyter nbconvert --execute notebooks/2-patch-extraction.ipynb

# 3. Feature extraction (UNI2-h, ~10 min/slide on Apple Silicon)
jupyter nbconvert --execute notebooks/3-feature-extraction-uni2h.ipynb

# 4. Train baseline classifier
jupyter nbconvert --execute notebooks/4-baseline-classifier-logreg.ipynb

# 5. Train ABMIL + generate heatmaps
jupyter nbconvert --execute notebooks/5-abmil-attention-mil.ipynb
```

### Useful Makefile targets

```bash
make lint       # ruff format --check && ruff check
make format     # ruff check --fix && ruff format
make clean      # delete __pycache__ and .pyc files
make data       # run data ingestion via typer CLI
```

---

## Reproducibility & Notes

- **Cross-validation:** 5-fold `GroupKFold` grouped by `case_id` (patient ID). This prevents data leakage from multiple slides of the same patient appearing in both train and validation sets. Fold splits are fully deterministic (no shuffle).
- **Threshold policy:** Fixed at 0.5 for all thresholded metrics (F1, sensitivity, specificity). No threshold tuning on validation data.
- **Ensemble inference:** All 5 fold checkpoints are loaded; each predicts on every test slide; probabilities are averaged; threshold 0.5 is applied.
- **Confounding check (LGG vs GBM):** TCGA-GBM is 100% mIDH&minus; and TCGA-LGG is ~70% mIDH+. To verify the model learns IDH morphology rather than tumour grade, a **LGG-only subgroup analysis** was performed: AUC 0.983 &pm; 0.034, confirming discrimination holds within LGG alone.
- **Batch effects:** t-SNE dual-coloring by IDH status and TCGA project showed no systematic batch separation, supporting the use of a single unified model.
- **PyTorch non-determinism:** Weight initialisation in `nn.Linear` and `max_patches` sampling introduce minor run-to-run variation. Set `torch.manual_seed(42)` before `create_fresh()` for reproducible training.

---

## Glossary

| Term | Definition |
|:-----|:-----------|
| **WSI** | Whole Slide Image &mdash; a high-resolution digital scan of a glass biopsy slide, typically several GB per file |
| **IDH** | Isocitrate Dehydrogenase &mdash; a metabolic enzyme. Mutations (mIDH+) are a key molecular marker in glioma prognosis |
| **mIDH+ / mIDH&minus;** | IDH mutant (positive prognosis marker) / IDH wildtype (negative prognosis marker) |
| **MIL** | Multiple Instance Learning &mdash; a framework where a "bag" of instances (patches) has a single bag-level label (slide diagnosis) |
| **ABMIL** | Attention-Based MIL &mdash; uses learned attention weights to aggregate patch features into a slide-level representation (Ilse et al., 2018) |
| **UNI2-h** | A vision transformer foundation model pre-trained on over 100 million histology patches, producing 1536-D feature vectors per patch |
| **Patch** | A small tile (256&times;256 pixels at 20&times; magnification) extracted from a tissue region of the WSI |
| **Attention weight** | A learned scalar per patch indicating how much that patch influences the slide-level prediction |
| **Contribution** | `attention_weight * dot(classifier_weight, embedding)` &mdash; a signed value showing both magnitude and direction of each patch's influence on the mIDH+ prediction |

---

## Acknowledgements

- **Data:** [The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/tcga) glioma cohort
- **Foundation model:** [UNI2-h](https://huggingface.co/MahmoodLab/UNI) (Mahmood Lab, Harvard)
- **Tissue processing:** [Trident](https://github.com/mahmoodlab/trident) library for segmentation, patching, and feature extraction
- **ABMIL architecture:** Ilse, M., Tomczak, J., & Welling, M. (2018). *Attention-based Deep Multiple Instance Learning.* ICML.
- **Project template:** [Cookiecutter Data Science](https://cookiecutter-data-science.drivendata.org/)
- **Assignment:** Computational Pathology "Hello World" exercise, Reichman University thesis programme

---

<p align="center"><em>Built by Daniel Levkovitz as part of a project at Reichman University.</em></p>
