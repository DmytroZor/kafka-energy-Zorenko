"""Convert the four editable markdown report texts into submission-friendly DOCX.

These are drafts that deliberately do not fabricate screenshots or performance metrics.
"""
import re
from pathlib import Path
from docx import Document
from docx.shared import Cm, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / 'results'


def rich(p, txt):
    parts=re.split(r'(\*\*[^*]+\*\*|`[^`]+`)',txt)
    for part in parts:
        if not part:
            continue
        if part.startswith('**') and part.endswith('**'):
            r=p.add_run(part[2:-2]);r.bold=True
        elif part.startswith('`') and part.endswith('`'):
            r=p.add_run(part[1:-1]);r.font.name='Consolas';r.font.size=Pt(9)
        else:
            p.add_run(part)


def add_cell_text(cell, txt, bold=False):
    p=cell.paragraphs[0]
    p.style='Normal'
    p.paragraph_format.space_after=Pt(3)
    rich(p,txt)
    for r in p.runs:
        r.font.size=Pt(9)
        if bold:r.bold=True
    cell.vertical_alignment=WD_CELL_VERTICAL_ALIGNMENT.CENTER


def make_doc(index, path):
    content=path.read_text(encoding='utf-8').splitlines()
    doc=Document()
    sec=doc.sections[0]
    sec.page_height=Cm(29.7);sec.page_width=Cm(21)
    sec.top_margin=Cm(2);sec.bottom_margin=Cm(2)
    sec.left_margin=Cm(2.4);sec.right_margin=Cm(1.7)
    styles=doc.styles
    normal=styles['Normal']
    normal.font.name='Times New Roman';normal.font.size=Pt(11)
    normal.paragraph_format.space_after=Pt(7)
    normal.paragraph_format.line_spacing=1.15
    for label,size in [('Title',20),('Heading 1',15),('Heading 2',12),('Heading 3',11)]:
        s=styles[label]
        s.font.name='Times New Roman';s.font.size=Pt(size);s.font.bold=True
        s.font.color.rgb=RGBColor(20,50,80)
        s.paragraph_format.space_before=Pt(12)
        s.paragraph_format.space_after=Pt(7)
        s.paragraph_format.keep_with_next=True
    # cover
    p=doc.add_paragraph()
    p.alignment=WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before=Pt(14)
    r=p.add_run('[ЗАКЛАД ВИЩОЇ ОСВІТИ]\n[ФАКУЛЬТЕТ ТА КАФЕДРА]')
    r.bold=True;r.font.size=Pt(12)
    p=doc.add_paragraph();p.alignment=WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before=Pt(110)
    r=p.add_run('ЗВІТ\nз практичної роботи №'+str(index))
    r.bold=True;r.font.size=Pt(20)
    p=doc.add_paragraph();p.alignment=WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before=Pt(15)
    # derive topic from first title text
    for ln in content:
        if ln.startswith('**Тема:**'):
            rich(p,ln);break
    p=doc.add_paragraph();p.alignment=WD_ALIGN_PARAGRAPH.RIGHT
    p.paragraph_format.space_before=Pt(80)
    rich(p,'Виконав: [ПІБ, група]')
    p=doc.add_paragraph();p.alignment=WD_ALIGN_PARAGRAPH.RIGHT
    rich(p,'Перевірив: [ПІБ викладача]')
    p=doc.add_paragraph();p.alignment=WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before=Pt(90)
    rich(p,'Київ — [рік виконання]')
    doc.add_page_break()
    i=0
    while i<len(content):
        line=content[i]
        if not line.strip():
            i+=1;continue
        if line.startswith('# '):
            doc.add_paragraph(line[2:].strip(),style='Title');i+=1;continue
        if line.startswith('## '):
            doc.add_paragraph(line[3:].strip(),style='Heading 1');i+=1;continue
        if line.startswith('### '):
            doc.add_paragraph(line[4:].strip(),style='Heading 2');i+=1;continue
        if line.startswith('```'):
            i+=1;code=[]
            while i<len(content) and not content[i].startswith('```'):
                code.append(content[i]);i+=1
            p=doc.add_paragraph()
            p.paragraph_format.left_indent=Cm(0.5)
            p.paragraph_format.space_after=Pt(8)
            for j,ln in enumerate(code):
                run=p.add_run(ln+'\n' if j<len(code)-1 else ln)
                run.font.name='Consolas';run.font.size=Pt(8)
            i+=1;continue
        if line.startswith('|') and i+1<len(content) and content[i+1].startswith('|'):
            rows=[]
            while i<len(content) and content[i].startswith('|'):
                cols=[x.strip() for x in content[i].strip().strip('|').split('|')]
                if not all(re.fullmatch(r':?-+:?',x) for x in cols):rows.append(cols)
                i+=1
            if rows:
                t=doc.add_table(rows=0,cols=len(rows[0]))
                t.style='Table Grid';t.alignment=WD_TABLE_ALIGNMENT.CENTER
                for ri,row in enumerate(rows):
                    cells=t.add_row().cells
                    for ci,x in enumerate(row):add_cell_text(cells[ci],x,ri==0)
                    if ri==0:
                        trPr=t.rows[ri]._tr.get_or_add_trPr()
                        el=OxmlElement('w:tblHeader');el.set(qn('w:val'),'true');trPr.append(el)
                doc.add_paragraph()
            continue
        if line.startswith('- '):
            p=doc.add_paragraph(style='List Bullet');rich(p,line[2:].strip())
            i+=1;continue
        p=doc.add_paragraph()
        rich(p,line.strip())
        i+=1
    footer=sec.footer.paragraphs[0]
    footer.alignment=WD_ALIGN_PARAGRAPH.CENTER
    footer.add_run('Практична робота №'+str(index)+'    •    сторінка ')
    field=OxmlElement('w:fldSimple');field.set(qn('w:instr'),'PAGE');footer._p.append(field)
    out=SOURCE/f'REPORT_0{index}.docx'
    doc.save(out)
    return out


if __name__=='__main__':
    for n in range(1,5):
        out=make_doc(n,SOURCE/f'REPORT_0{n}.md')
        print(out, out.stat().st_size)
