"""Add an introduction slide to the completed adapter/resolver visual deck."""

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

from create_adapter_resolver_visual import (
    CHEMBL,
    CHEMBL_PALE,
    DC,
    DC_PALE,
    INK,
    LINE,
    MERGE,
    MERGE_PALE,
    MUTED,
    NAVY,
    RESOLVER,
    RESOLVER_PALE,
    WHITE,
    arrow,
    box,
    text,
)


HERE = Path(__file__).parent
SOURCE = HERE / "adapter_resolver_visual_complete.pptx"
OUT = HERE / "adapter_resolver_boundary_deck.pptx"
LOSS = RGBColor(184, 67, 67)
LOSS_PALE = RGBColor(250, 232, 230)


def add_intro_slide(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])

    text(slide, "Why separate “who” from “what”?",
         .64, .48, 12.05, .62, 28, NAVY, True, PP_ALIGN.CENTER)
    text(slide,
         "We want to show that separating identity resolution from source-data modeling\ncreates a more flexible and maintainable system.",
         1.18, 1.22, 10.98, .78, 17, MUTED, False, PP_ALIGN.CENTER)

    box(slide, 1.00, 2.35, 4.78, 2.76, RESOLVER_PALE, RESOLVER)
    text(slide, "WHO?", 1.35, 2.68, 4.08, .44,
         25, RESOLVER, True, PP_ALIGN.CENTER)
    text(slide, "ID Resolver", 1.35, 3.20, 4.08, .35,
         17, NAVY, True, PP_ALIGN.CENTER)
    text(slide,
         "Which source records refer to\nthe same real-world entity?",
         1.48, 3.77, 3.82, .70, 16, INK, False, PP_ALIGN.CENTER)
    box(slide, 1.69, 4.55, 3.40, .34, WHITE, RESOLVER)
    text(slide, "Maps source IDs → preferred ID", 1.84, 4.61, 3.10, .20,
         11.5, RESOLVER, True, PP_ALIGN.CENTER)

    box(slide, 7.55, 2.35, 4.78, 2.76, DC_PALE, DC)
    text(slide, "WHAT?", 7.90, 2.68, 4.08, .44,
         25, DC, True, PP_ALIGN.CENTER)
    text(slide, "Source Adapter", 7.90, 3.20, 4.08, .35,
         17, NAVY, True, PP_ALIGN.CENTER)
    text(slide,
         "What fields and relationships does\nthis source contribute to the graph?",
         8.03, 3.77, 3.82, .70, 16, INK, False, PP_ALIGN.CENTER)
    box(slide, 8.24, 4.55, 3.40, .34, WHITE, DC)
    text(slide, "Maps source payload → ODIN model", 8.39, 4.61, 3.10, .20,
         11.5, DC, True, PP_ALIGN.CENTER)

    arrow(slide, 5.96, 3.73, 7.37, 3.73, MUTED, 2.1)
    text(slide, "independent,\ncomposable roles", 6.01, 3.15, 1.30, .50,
         10.5, MUTED, True, PP_ALIGN.CENTER)

    box(slide, 1.00, 5.61, 11.33, 1.10, WHITE, NAVY)
    text(slide, "When these responsibilities stay separate:",
         1.30, 5.79, 3.24, .30, 13, NAVY, True)
    text(slide, "Change source fields without rebuilding identity logic",
         4.55, 5.78, 2.40, .43, 11.2, CHEMBL, True, PP_ALIGN.CENTER)
    text(slide, "Reuse the same identities across different graphs",
         7.10, 5.78, 2.28, .43, 11.2, RESOLVER, True, PP_ALIGN.CENTER)
    text(slide, "Preserve source-level evidence and auditability",
         9.56, 5.78, 2.46, .43, 11.2, DC, True, PP_ALIGN.CENTER)

    text(slide,
         "The following examples show the boundary working well—and what happens when it is collapsed.",
         1.16, 6.95, 11.02, .30, 13.5, NAVY, True, PP_ALIGN.CENTER)

    # python-pptx appends slides; move this new slide to the beginning.
    slide_ids = prs.slides._sldIdLst
    new_slide_id = slide_ids[-1]
    slide_ids.remove(new_slide_id)
    slide_ids.insert(0, new_slide_id)


def loss_row(slide, number, heading, body, y):
    badge = slide.shapes.add_shape(
        MSO_SHAPE.OVAL, Inches(.92), Inches(y), Inches(.37), Inches(.37)
    )
    badge.fill.solid()
    badge.fill.fore_color.rgb = LOSS
    badge.line.fill.background()
    text(slide, str(number), .99, y + .055, .23, .22,
         9.5, WHITE, True, PP_ALIGN.CENTER)
    text(slide, heading, 1.48, y - .01, 1.35, .25,
         11.2, LOSS, True)
    text(slide, body, 2.82, y - .01, 2.03, .44,
         9.8, INK)


def add_summary_slide(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])

    text(slide, "A better handoff preserves both harmonization and source independence",
         .52, .27, 12.28, .52, 23.5, NAVY, True)
    text(slide,
         "The problem is not harmonized data—it is using one combined export to replace independently ingestible sources.",
         .54, .84, 12.10, .32, 13, MUTED)
    rule = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(.55), Inches(1.25), Inches(.92), Inches(.04)
    )
    rule.fill.solid()
    rule.fill.fore_color.rgb = RESOLVER
    rule.line.fill.background()

    # What is lost
    box(slide, .55, 1.55, 4.62, 4.88, LOSS_PALE, LOSS)
    text(slide, "WHEN A COMBINED EXPORT\nREPLACES SOURCE ADAPTERS", .88, 1.78, 3.96, .57,
         15, LOSS, True, PP_ALIGN.CENTER)
    loss_row(slide, 1, "Source choice", "Cannot independently include, exclude, or version each provider.", 2.62)
    loss_row(slide, 2, "Audit trail", "Field ownership collapses into a single “Drug Harmonizer” source.", 3.27)
    loss_row(slide, 3, "Payload access", "Fields omitted upstream are unavailable to ODIN.", 3.92)
    loss_row(slide, 4, "Change isolation", "A source-field change requires another Harmonizer export.", 4.57)
    loss_row(slide, 5, "Use-case freedom", "Target Graph and Pharos inherit the same upstream choices.", 5.22)
    text(slide, "Identity, payload, and graph policy become one release unit.",
         .92, 5.92, 3.88, .28, 10.5, LOSS, True, PP_ALIGN.CENTER)

    # Better boundary
    text(slide, "USE THE HARMONIZER OUTPUT TWO WAYS", 5.55, 1.57, 7.27, .32,
         15, NAVY, True, PP_ALIGN.CENTER)

    box(slide, 5.55, 2.05, 2.02, 1.20, MERGE_PALE, MERGE)
    text(slide, "HARMONIZER OUTPUT", 5.72, 2.20, 1.68, .22,
         9.5, MERGE, True, PP_ALIGN.CENTER)
    text(slide, "drug_ids.tsv", 5.72, 2.51, 1.68, .30,
         14, MERGE, True, PP_ALIGN.CENTER)
    text(slide, "IDs · scores · methods", 5.72, 2.88, 1.68, .20,
         9.2, MUTED, False, PP_ALIGN.CENTER)

    box(slide, 8.00, 1.98, 2.08, 1.26, RESOLVER_PALE, RESOLVER)
    text(slide, "ID RESOLVER — WHO", 8.18, 2.16, 1.72, .28,
         11.5, RESOLVER, True, PP_ALIGN.CENTER)
    text(slide, "Source IDs → preferred ID", 8.18, 2.61, 1.72, .25,
         9.8, INK, False, PP_ALIGN.CENTER)

    box(slide, 8.00, 3.52, 2.08, 1.26, CHEMBL_PALE, CHEMBL)
    text(slide, "HARMONIZER ADAPTER", 8.18, 3.70, 1.72, .28,
         11.0, CHEMBL, True, PP_ALIGN.CENTER)
    text(slide, "Scores · methods · decisions", 8.18, 4.15, 1.72, .25,
         9.6, INK, False, PP_ALIGN.CENTER)

    box(slide, 5.55, 4.98, 2.02, 1.20, DC_PALE, DC)
    text(slide, "PRIMARY SOURCES", 5.72, 5.16, 1.68, .24,
         10.5, DC, True, PP_ALIGN.CENTER)
    text(slide, "DrugCentral · ChEMBL\nIUPHAR · others", 5.72, 5.52, 1.68, .46,
         10.0, INK, False, PP_ALIGN.CENTER)

    box(slide, 8.00, 4.98, 2.08, 1.20, DC_PALE, DC)
    text(slide, "SOURCE ADAPTERS", 8.18, 5.16, 1.72, .24,
         10.8, DC, True, PP_ALIGN.CENTER)
    text(slide, "Independent fields + evidence", 8.18, 5.53, 1.72, .25,
         9.5, INK, False, PP_ALIGN.CENTER)

    box(slide, 10.55, 2.10, 2.28, 4.05, WHITE, NAVY)
    text(slide, "ODIN GRAPH", 10.77, 2.32, 1.84, .30,
         15, NAVY, True, PP_ALIGN.CENTER)
    box(slide, 10.88, 2.93, 1.62, .60, RESOLVER_PALE, RESOLVER)
    text(slide, "Preferred identity", 11.01, 3.09, 1.36, .24,
         10.0, RESOLVER, True, PP_ALIGN.CENTER)
    box(slide, 10.88, 3.78, 1.62, .80, CHEMBL_PALE, CHEMBL)
    text(slide, "Harmonizer scores\n+ methods", 11.01, 3.91, 1.36, .50,
         9.8, CHEMBL, True, PP_ALIGN.CENTER)
    box(slide, 10.88, 4.84, 1.62, .91, DC_PALE, DC)
    text(slide, "Primary-source fields\n+ provenance", 11.01, 5.00, 1.36, .56,
         9.8, DC, True, PP_ALIGN.CENTER)

    arrow(slide, 7.61, 2.40, 7.96, 2.40, RESOLVER)
    arrow(slide, 7.27, 3.27, 7.96, 4.03, CHEMBL)
    arrow(slide, 7.61, 5.58, 7.96, 5.58, DC)
    arrow(slide, 10.12, 2.61, 10.51, 3.22, RESOLVER)
    arrow(slide, 10.12, 4.15, 10.51, 4.16, CHEMBL)
    arrow(slide, 10.12, 5.58, 10.51, 5.28, DC)

    box(slide, .55, 6.72, 12.28, .50, RESOLVER_PALE, RESOLVER)
    text(slide, "Harmonizer decides identity. ODIN decides graph content.",
         .80, 6.82, 11.78, .28, 13.5, RESOLVER, True, PP_ALIGN.CENTER)


def main():
    prs = Presentation(SOURCE)
    add_intro_slide(prs)
    add_summary_slide(prs)
    prs.core_properties.title = "Why ODIN separates adapters from ID resolvers"
    prs.save(OUT)
    print(OUT)


if __name__ == "__main__":
    main()
