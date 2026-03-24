const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, BorderStyle, WidthType, ShadingType,
  LevelFormat, ExternalHyperlink, VerticalAlign, Footer, PageNumber
} = require('docx');
const fs = require('fs');

// Provider flags are read by the report agent before text replacement.
const AI_PROVIDER = process.env.AI_PROVIDER || '';
const HAS_ANTHROPIC = AI_PROVIDER === 'anthropic' && !!process.env.ANTHROPIC_API_KEY;

// ── Palette ──
const C = {
  navy:     '1B3A6B',
  blue:     '2563A8',
  lblue:    'DBEAFE',
  critical: 'B91C1C', critBg:  'FEE2E2',
  high:     'C2410C', highBg:  'FFEDD5',
  green:    '166534', greenBg: 'DCFCE7',
  purple:   '6B21A8', purpleBg:'F3E8FF',
  slate:    '475569', slBg:    'F1F5F9',
  border:   'CBD5E1',
  white:    'FFFFFF',
  text:     '1E293B',
  muted:    '64748B',
};

const b   = (col, sz=1) => ({ style: BorderStyle.SINGLE, size: sz, color: col });
const nb  = () => ({ style: BorderStyle.NONE, size: 0, color: 'FFFFFF' });
const allB= (col) => ({ top:b(col), bottom:b(col), left:b(col), right:b(col) });
const noB = () => ({ top:nb(), bottom:nb(), left:nb(), right:nb() });
const sp  = (bef=0, aft=0) => ({ spacing:{ before:bef, after:aft } });

const r = (text, o={}) => new TextRun({ text, font:'Arial', size:18, color:C.text, ...o });
const gap = (h=80) => new Paragraph({ spacing:{before:h,after:0}, children:[new TextRun('')] });

// Blue ALL-CAPS section label with bottom border
function sectionLabel(text) {
  return new Paragraph({
    spacing:{ before:160, after:60 },
    border:{ bottom: b(C.blue, 3) },
    children:[new TextRun({ text, font:'Arial', size:18, bold:true, color:C.blue, allCaps:true })]
  });
}

// KPI card cell
function kpiCell(value, label, numColor, bg, w) {
  return new TableCell({
    borders: allB(numColor),
    width:{ size:w, type:WidthType.DXA },
    shading:{ fill:bg, type:ShadingType.CLEAR },
    margins:{ top:120, bottom:120, left:140, right:140 },
    children:[
      new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,30),
        children:[new TextRun({ text:String(value), font:'Arial', size:48, bold:true, color:numColor })] }),
      new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,0),
        children:[new TextRun({ text:label, font:'Arial', size:15, color:C.muted })] }),
    ]
  });
}

// Simple stat table row
function statRow(label, value, valColor, bg) {
  return new TableRow({ children:[
    new TableCell({
      borders: allB(C.border),
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60, bottom:60, left:120, right:80 },
      children:[new Paragraph({ ...sp(0,0), children:[r(label, {size:17})] })]
    }),
    new TableCell({
      borders: allB(C.border),
      width:{ size:560, type:WidthType.DXA },
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60, bottom:60, left:80, right:100 },
      children:[new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,0),
        children:[r(String(value), {bold:true, color:valColor||C.navy, size:17})] })]
    }),
  ]});
}

function makeStatTable(rows, colWidth) {
  return new Table({
    width:{ size:colWidth, type:WidthType.DXA },
    columnWidths:[ colWidth-560, 560 ],
    rows
  });
}

// Trend row: #N | description | count
function trendRow(rank, text, count, accentColor, bg) {
  return new TableRow({ children:[
    new TableCell({
      borders: allB(C.border),
      width:{ size:340, type:WidthType.DXA },
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:100,right:60 },
      verticalAlign: VerticalAlign.CENTER,
      children:[new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,0),
        children:[r(String(rank), {bold:true, color:accentColor||C.muted, size:15})] })]
    }),
    new TableCell({
      borders: allB(C.border),
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:100,right:80 },
      children:[new Paragraph({ ...sp(0,0), children:[r(text, {size:16, color:C.text})] })]
    }),
    new TableCell({
      borders: allB(C.border),
      width:{ size:440, type:WidthType.DXA },
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:80,right:100 },
      children:[new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,0),
        children:[r(String(count), {bold:true, color:accentColor||C.navy, size:17})] })]
    }),
  ]});
}

// Host row: #N | hostname | count | type-label
function hostRow(rank, host, count, typeLabel, accentColor, bg) {
  return new TableRow({ children:[
    new TableCell({
      borders: allB(C.border),
      width:{ size:340, type:WidthType.DXA },
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:100,right:60 },
      children:[new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,0),
        children:[r(String(rank), {color:accentColor||C.muted, size:15})] })]
    }),
    new TableCell({
      borders: allB(C.border),
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:100,right:80 },
      children:[new Paragraph({ ...sp(0,0),
        children:[r(host, {size:16, bold:accentColor===C.critical, color:C.text})] })]
    }),
    new TableCell({
      borders: allB(C.border),
      width:{ size:380, type:WidthType.DXA },
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:80,right:80 },
      children:[new Paragraph({ alignment:AlignmentType.CENTER, ...sp(0,0),
        children:[r(String(count), {bold:true, color:accentColor||C.navy, size:17})] })]
    }),
    new TableCell({
      borders: allB(C.border),
      width:{ size:740, type:WidthType.DXA },
      shading:{ fill:bg||C.white, type:ShadingType.CLEAR },
      margins:{ top:60,bottom:60,left:80,right:100 },
      children:[new Paragraph({ ...sp(0,0),
        children:[r(typeLabel, {size:15, color:accentColor||C.muted})] })]
    }),
  ]});
}

// Feedback table row (3 cols: field | count | status)
function feedbackRow(field, count, status, fieldBold, fieldColor, fieldBg, statusColor) {
  const bg = fieldBg||C.white;
  return new TableRow({ children:[
    new TableCell({ borders:allB(C.border), shading:{fill:bg,type:ShadingType.CLEAR},
      margins:{top:60,bottom:60,left:120,right:80},
      children:[new Paragraph({...sp(0,0), children:[r(field,{size:17,bold:!!fieldBold,color:fieldColor||C.text})]})]
    }),
    new TableCell({ borders:allB(C.border), shading:{fill:bg,type:ShadingType.CLEAR},
      width:{size:600,type:WidthType.DXA},
      margins:{top:60,bottom:60,left:80,right:80},
      children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),
        children:[r(String(count),{size:17,bold:true,color:fieldColor||C.muted})]})]
    }),
    new TableCell({ borders:allB(C.border), shading:{fill:bg,type:ShadingType.CLEAR},
      width:{size:660,type:WidthType.DXA},
      margins:{top:60,bottom:60,left:80,right:120},
      children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),
        children:[r(status||'\u2014',{size:16,bold:!!statusColor,color:statusColor||C.muted})]})]
    }),
  ]});
}

// ── LAYOUT ──
const PAGE_W  = 12240;
const MARGIN  = 720;
const CONTENT = PAGE_W - MARGIN*2; // 10800
const LEFT_W  = 5160;
const GAP_W   = 200;
const RIGHT_W = CONTENT - LEFT_W - GAP_W; // 5440

const doc = new Document({
  numbering:{
    config:[{reference:'bullets',levels:[{level:0,format:LevelFormat.BULLET,text:'\u2022',
      alignment:AlignmentType.LEFT,style:{paragraph:{indent:{left:480,hanging:240}}}}]}]
  },
  styles:{
    default:{ document:{ run:{ font:'Arial', size:18, color:C.text } } },
  },
  sections:[{
    properties:{
      page:{
        size:{ width:PAGE_W, height:15840 },
        margin:{ top:MARGIN, right:MARGIN, bottom:MARGIN, left:MARGIN }
      }
    },
    footers:{
      default: new Footer({ children:[
        new Paragraph({
          alignment:AlignmentType.RIGHT,
          spacing:{before:80,after:0},
          children:[
            r('Page ', {size:17, color:C.muted}),
            new TextRun({ children:[PageNumber.CURRENT], font:'Arial', size:17, color:C.muted }),
            r(' / ', {size:17, color:C.muted}),
            new TextRun({ children:[PageNumber.TOTAL_PAGES], font:'Arial', size:17, color:C.muted }),
          ]
        })
      ]})
    },

    children:[

      // ══════════════════════════════════════
      // HEADER BAR
      // ══════════════════════════════════════
      new Table({
        width:{size:CONTENT, type:WidthType.DXA},
        columnWidths:[7000, CONTENT-7000],
        rows:[new TableRow({ children:[
          new TableCell({
            borders:noB(), shading:{fill:C.navy,type:ShadingType.CLEAR},
            margins:{top:200,bottom:200,left:280,right:160},
            children:[
              new Paragraph({...sp(0,40),
                children:[new TextRun({text:'Rocket Companies',font:'Arial',size:17,color:'93C5FD'})]}),
              new Paragraph({...sp(0,0),
                children:[new TextRun({text:'Automated Alert Triage \u2014 POC Report',font:'Arial',size:34,bold:true,color:C.white})]}),
            ]
          }),
          new TableCell({
            borders:noB(), shading:{fill:C.navy,type:ShadingType.CLEAR},
            margins:{top:200,bottom:200,left:160,right:280},
            verticalAlign:VerticalAlign.CENTER,
            children:[
              new Paragraph({alignment:AlignmentType.RIGHT,...sp(0,40),
                children:[new TextRun({text:'Generated: 2026-03-13',font:'Arial',size:16,color:'93C5FD'})]}),
              new Paragraph({alignment:AlignmentType.RIGHT,...sp(0,0),
                children:[
                  new TextRun({text:'Scope: 17 alerts',font:'Arial',size:16,color:'BFDBFE'}),
                  new TextRun({text:'  \u2502  Mar 13, 2026',font:'Arial',size:16,color:'BFDBFE'}),
                ]}),
            ]
          }),
        ]})]
      }),
      gap(120),

      // ══════════════════════════════════════
      // KPI ROW — 5 cards
      // ══════════════════════════════════════
      new Table({
        width:{size:CONTENT, type:WidthType.DXA},
        columnWidths:[2160,2160,2160,2160,CONTENT-2160*4],
        rows:[new TableRow({ children:[
          kpiCell('17', 'Alerts Triaged',          C.navy,     C.lblue,    2160),
          kpiCell('10', 'Auto-Closed',             C.green,    C.greenBg,  2160),
          kpiCell('6',  'Escalate Immediately',    C.critical, C.critBg,   2160),
          kpiCell('1',  'Escalate for Review',     C.high,     C.highBg,   2160),
          kpiCell('2',  'Feedback Loop',           C.purple,   C.purpleBg, CONTENT-2160*4),
        ]})]
      }),
      gap(120),

      // ══════════════════════════════════════
      // BODY: two columns
      // ══════════════════════════════════════
      new Table({
        width:{size:CONTENT, type:WidthType.DXA},
        columnWidths:[LEFT_W, GAP_W, RIGHT_W],
        rows:[new TableRow({ children:[

          // ────────────────────────────────
          // LEFT COLUMN
          // ────────────────────────────────
          new TableCell({ borders:noB(), width:{size:LEFT_W,type:WidthType.DXA},
            children:[

              // EXECUTIVE SUMMARY
              sectionLabel('Executive Summary'),
              new Paragraph({...sp(60,100),
                children:[
                  r('On March 13, 2026, the automated triage system processed '),
                  r('17 CrowdStrike alerts', {bold:true}),
                  r('. Six critical threats were escalated immediately, ten benign alerts were auto-closed, and one borderline case was routed for human review. '),
                  r('2 analyst verdict overrides', {bold:true, color:C.purple}),
                  r(' were recorded on XMCHELAP408 — both web-shell alerts declined as known XOME TM processes by '),
                  r('adamlarkin@rocket.com', {bold:true}),
                  r('. The system delivered high-confidence verdicts on every alert processed.'),
                ]
              }),

              // ALERT STATISTICS
              sectionLabel('Alert Statistics'),
              gap(60),

              // Side-by-side: Final Decisions | Primary Assessment
              new Table({
                width:{size:LEFT_W, type:WidthType.DXA},
                columnWidths:[2440, 200, 2520],
                rows:[new TableRow({ children:[
                  // Final Decisions mini-table
                  new TableCell({ borders:noB(), width:{size:2440,type:WidthType.DXA},
                    children:[
                      new Paragraph({...sp(0,50),children:[r('Final Decisions',{size:17,color:C.muted})]}),
                      makeStatTable([
                        statRow('Escalate Immediately', 6, C.critical, C.critBg),
                        statRow('Escalate for Review',  1, C.high,     C.highBg),
                        statRow('Close',               10, C.green,    C.greenBg),
                      ], 2440)
                    ]
                  }),
                  new TableCell({ borders:noB(), width:{size:200,type:WidthType.DXA}, children:[gap()] }),
                  // Primary Assessment mini-table
                  new TableCell({ borders:noB(), width:{size:2520,type:WidthType.DXA},
                    children:[
                      new Paragraph({...sp(0,50),children:[r('Primary Assessment',{size:17,color:C.muted})]}),
                      makeStatTable([
                        statRow('Confirmed Malicious',      4, C.critical, C.critBg),
                        statRow('High-Conf. Suspicious',    2, C.high,     C.highBg),
                        statRow('Anomalous but Benign',    11, C.green,    C.greenBg),
                      ], 2520)
                    ]
                  }),
                ]})]
              }),


            ]
          }),

          // GUTTER
          new TableCell({ borders:noB(), width:{size:GAP_W,type:WidthType.DXA}, children:[gap()] }),

          // ────────────────────────────────
          // RIGHT COLUMN
          // ────────────────────────────────
          new TableCell({ borders:noB(), width:{size:RIGHT_W,type:WidthType.DXA},
            children:[

              // FEEDBACK & VERDICT CHANGES (combined)
              sectionLabel('Feedback & Verdict Changes'),
              gap(60),

              // Feedback counts table
              new Table({
                width:{size:RIGHT_W,type:WidthType.DXA},
                columnWidths:[RIGHT_W-600-660, 600, 660],
                rows:[
                  new TableRow({ tableHeader:true, children:[
                    new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
                      margins:{top:70,bottom:70,left:120,right:80},
                      children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Feedback Field',font:'Arial',size:17,bold:true,color:C.white})]})] }),
                    new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
                      width:{size:600,type:WidthType.DXA},
                      margins:{top:70,bottom:70,left:80,right:80},
                      children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[new TextRun({text:'Count',font:'Arial',size:17,bold:true,color:C.white})]})] }),
                    new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
                      width:{size:660,type:WidthType.DXA},
                      margins:{top:70,bottom:70,left:80,right:120},
                      children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[new TextRun({text:'Status',font:'Arial',size:17,bold:true,color:C.white})]})] }),
                  ]}),
                  feedbackRow('human_verified',     0, '\u2014', false, C.muted,  C.white,   null),
                  feedbackRow('verdict_modified',   2, 'Active',  true,  C.purple, C.purpleBg, C.purple),
                  feedbackRow('verdict_restored',   0, '\u2014', false, C.muted,  C.white,   null),
                  feedbackRow('verification_undone',0, '\u2014', false, C.muted,  C.white,   null),
                ]
              }),
              gap(80),

              // Combined declined card with rubric inline
              new Table({
                width:{size:RIGHT_W,type:WidthType.DXA},
                columnWidths:[RIGHT_W],
                rows:[new TableRow({ children:[new TableCell({
                  borders: allB(C.purple),
                  shading:{ fill:C.purpleBg, type:ShadingType.CLEAR },
                  margins:{ top:140, bottom:140, left:200, right:200 },
                  children:[
                    // Title row
                    new Paragraph({...sp(0,80),
                      children:[
                        new TextRun({text:'\u26A0  Verdict Declined  \u2014  ',font:'Arial',size:18,bold:true,color:C.purple}),
                        new TextRun({text:'False Positive',font:'Arial',size:18,bold:true,color:C.critical}),
                        new TextRun({text:'  (2 confirmed)',font:'Arial',size:16,color:C.purple}),
                      ]}),
                    // Rubric table embedded in card
                    new Table({
                      width:{size:RIGHT_W-400,type:WidthType.DXA},
                      columnWidths:[800, RIGHT_W-400-800-1200-1100, 1200, 1100],
                      rows:[
                        new TableRow({ tableHeader:true, children:[
                          new TableCell({ borders:allB('9B7733'), shading:{fill:'9B7733',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:80,right:60},
                            children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Alert',font:'Arial',size:15,bold:true,color:'FFFFFF'})]})] }),
                          new TableCell({ borders:allB('9B7733'), shading:{fill:'9B7733',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:60},
                            children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Original',font:'Arial',size:15,bold:true,color:'FFFFFF'})]})] }),
                          new TableCell({ borders:allB('9B7733'), shading:{fill:'9B7733',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:60},
                            children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Updated',font:'Arial',size:15,bold:true,color:'FFFFFF'})]})] }),
                          new TableCell({ borders:allB('9B7733'), shading:{fill:'9B7733',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:80},
                            children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Confirmation',font:'Arial',size:15,bold:true,color:'FFFFFF'})]})] }),
                        ]}),
                        new TableRow({ children:[
                          new TableCell({ borders:allB(C.border), shading:{fill:'FFFFFF',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:80,right:60},
                            children:[new Paragraph({...sp(0,0),children:[r('#79',{size:15,bold:true,color:C.purple})]})] }),
                          new TableCell({ borders:allB(C.border), shading:{fill:'FFFFFF',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:60},
                            children:[new Paragraph({...sp(0,0),children:[r('TP \u2014 Malicious',{size:14,color:C.critical})]})] }),
                          new TableCell({ borders:allB(C.border), shading:{fill:'FFFFFF',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:60},
                            children:[new Paragraph({...sp(0,0),children:[r('TP \u2014 Benign',{size:14,color:C.green})]})] }),
                          new TableCell({ borders:allB(C.border), shading:{fill:'FFFFFF',type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:80},
                            children:[new Paragraph({...sp(0,0),children:[r('Declined',{size:14,bold:true,color:C.purple})]})] }),
                        ]}),
                        new TableRow({ children:[
                          new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:80,right:60},
                            children:[new Paragraph({...sp(0,0),children:[r('#78',{size:15,bold:true,color:C.purple})]})] }),
                          new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:60},
                            children:[new Paragraph({...sp(0,0),children:[r('TP \u2014 Malicious',{size:14,color:C.critical})]})] }),
                          new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:60},
                            children:[new Paragraph({...sp(0,0),children:[r('TP \u2014 Benign',{size:14,color:C.green})]})] }),
                          new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR},
                            margins:{top:50,bottom:50,left:60,right:80},
                            children:[new Paragraph({...sp(0,0),children:[r('Declined',{size:14,bold:true,color:C.purple})]})] }),
                        ]}),
                      ]
                    }),
                    gap(60),
                    new Paragraph({...sp(0,40),
                      children:[
                        r('By: ',{size:15,bold:true,color:C.slate}),
                        r('adamlarkin@rocket.com',{size:15,color:C.blue}),
                        r('   \u2502   Mar 13  15:53 / 15:56',{size:15,color:C.muted}),
                      ]}),
                    new Paragraph({...sp(0,60),
                      children:[r('\u201CThis is a known process for XOME TMs.\u201D',{size:15,italics:true,color:C.slate})]}),
                    new Paragraph({...sp(0,50),
                      children:[
                        r('Langfuse: ',{size:14,bold:true,color:C.slate}),
                        new ExternalHyperlink({
                          link:'https://langfuse.us.torqio.dev/project/cmg7q573p0001wv07rsw58dbu/sessions/019ce78f-2540-7c07-83b8-d6f3fc038dcd',
                          children:[new TextRun({text:'019ce78f (Alert #79)',font:'Arial',size:14,color:C.blue,underline:{}})]
                        }),
                        new TextRun({text:'  \u2502  ',font:'Arial',size:14,color:C.muted}),
                        new ExternalHyperlink({
                          link:'https://langfuse.us.torqio.dev/project/cmg7q573p0001wv07rsw58dbu/sessions/019ce740-7cf6-7b9d-98c6-f7fe754a6fbe',
                          children:[new TextRun({text:'019ce740 (Alert #78)',font:'Arial',size:14,color:C.blue,underline:{}})]
                        }),
                      ]}),


                  ]
                })]})],
              }),
              gap(80),

              // ALERTS BY VERDICT
              sectionLabel('Alerts by Verdict'),
              gap(60),

              new Table({
                width:{size:RIGHT_W,type:WidthType.DXA},
                columnWidths:[RIGHT_W-700,700],
                rows:[
                  statRow('True Positive \u2014 Benign',   11, C.green,    C.greenBg),
                  statRow('True Positive \u2014 Malicious', 6, C.critical, C.critBg),
                ]
              }),

            ]
          }),

        ]})]
      }),

      gap(120),

      // ══════════════════════════════════════
      // REPEATED TRENDS — full width
      // ══════════════════════════════════════
      sectionLabel('Repeated Trends'),
      gap(60),

      new Table({
        width:{size:CONTENT, type:WidthType.DXA},
        columnWidths:[300, 1800, 500, CONTENT-300-1800-500-500, 500],
        rows:[
          new TableRow({ tableHeader:true, children:[
            new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
              width:{size:300,type:WidthType.DXA}, margins:{top:70,bottom:70,left:80,right:60},
              children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'#',font:'Arial',size:16,bold:true,color:C.white})]})] }),
            new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
              width:{size:1800,type:WidthType.DXA}, margins:{top:70,bottom:70,left:80,right:60},
              children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Host',font:'Arial',size:16,bold:true,color:C.white})]})] }),
            new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
              width:{size:500,type:WidthType.DXA}, margins:{top:70,bottom:70,left:60,right:60},
              children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[new TextRun({text:'Alerts',font:'Arial',size:16,bold:true,color:C.white})]})] }),
            new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
              margins:{top:70,bottom:70,left:80,right:60},
              children:[new Paragraph({...sp(0,0),children:[new TextRun({text:'Alert Type',font:'Arial',size:16,bold:true,color:C.white})]})] }),
            new TableCell({ borders:allB(C.navy), shading:{fill:C.navy,type:ShadingType.CLEAR},
              width:{size:500,type:WidthType.DXA}, margins:{top:70,bottom:70,left:60,right:80},
              children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[new TextRun({text:'Count',font:'Arial',size:16,bold:true,color:C.white})]})] }),
          ]}),
          new TableRow({ children:[
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:300,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('1',{bold:true,color:C.critical,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:1800,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('XMCHELAP408',{bold:true,color:C.critical,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{bold:true,color:C.critical,size:17})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('Web shell — Declined (known XOME TM process)',{size:16,color:C.critical})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:80}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{bold:true,color:C.critical,size:17})]})]}),
          ]}),
          new TableRow({ children:[
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:300,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{color:C.critical,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:1800,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('XMCHELAP507',{color:C.critical,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{bold:true,color:C.critical,size:17})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('Web shell activity',{size:16,color:C.critical})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.critBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:80}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{bold:true,color:C.critical,size:17})]})]}),
          ]}),
          new TableRow({ children:[
            new TableCell({ borders:allB(C.border), shading:{fill:C.highBg,type:ShadingType.CLEAR}, width:{size:300,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('3',{color:C.high,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.highBg,type:ShadingType.CLEAR}, width:{size:1800,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('MACCC02WP0EUHTD8',{color:C.high,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.highBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('3',{bold:true,color:C.high,size:17})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.highBg,type:ShadingType.CLEAR}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('Empyre Backdoor',{size:16,color:C.high})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.highBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:80}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('3',{bold:true,color:C.high,size:17})]})]}),
          ]}),
          new TableRow({ children:[
            new TableCell({ borders:allB(C.border), width:{size:300,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('4',{color:C.muted,size:16})]})]}),
            new TableCell({ borders:allB(C.border), width:{size:1800,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('5CG43966WS',{size:16})]})]}),
            new TableCell({ borders:allB(C.border), width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{bold:true,size:17})]})]}),
            new TableCell({ borders:allB(C.border), margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('ML low-confidence file detection',{size:16,color:C.muted})]})]}),
            new TableCell({ borders:allB(C.border), width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:80}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('2',{bold:true,size:17})]})]}),
          ]}),
          new TableRow({ children:[
            new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR}, width:{size:300,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('5',{color:C.muted,size:16})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR}, width:{size:1800,type:WidthType.DXA}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('Various',{size:16,color:C.muted})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:60}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('8',{bold:true,color:C.muted,size:17})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR}, margins:{top:60,bottom:60,left:80,right:60}, children:[new Paragraph({...sp(0,0),children:[r('Custom rule / Msiexec',{size:16,color:C.muted})]})]}),
            new TableCell({ borders:allB(C.border), shading:{fill:C.slBg,type:ShadingType.CLEAR}, width:{size:500,type:WidthType.DXA}, margins:{top:60,bottom:60,left:60,right:80}, children:[new Paragraph({alignment:AlignmentType.CENTER,...sp(0,0),children:[r('8',{bold:true,color:C.muted,size:17})]})]}),
          ]}),
        ]
      }),

    ]
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync('/home/claude/rocket_visual_mar13.docx', buf);
  console.log('Done');
});
