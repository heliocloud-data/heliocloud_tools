; usage: replace all plot,... with jnplot,...
;    and oplot,... with jnplot,/overplot,...
;(where common block jn_plot is persistent and used in future plots)
; There are around 7000 plot/oplot/xyouts/set_plot/tvscl/tv
; see also ssw/gen/idl/display and its tv2.plro, xyouts2.pro, etc
; Note plot alone has 42 possible keywords

; Later possibility-- rewrite coyote's cgplot for our use:
; http://www.idlcoyote.com/graphics_tips/coyote_graphics.php

; plot object lacks isotropic, nsum, ynozero
; plot variables, core = [x], y
; (* = also in oplot)
; flags: isotropic,*polar,xlog,ylog,ynozero
; values: max_value,min_value,nsum,thick
; graphics flags: data|device,normal,*noclip,nodata,noerase,*t3d
; graphics keywords: title,background,charsize,charthick,font,position,subtitle,thick,ticklen,
;  *clip,*color,*linestyle,*psym,*symsize,*zvalue
; also the x|y|zVARS: charsize,gridstyle,xmargin,minor,range,style,thick,tick_get,tickformat,tickinterval,
;    ticklayout,ticklen,tickname,ticks,tickunits,tickv,title
; plot objects that correlate: core of [x],y
;   format=string or set as *ymbol=psym,color=color,linestyle=linestyle,thick=thick
;   /overplot
;   color,linestyle,max_value,min_value,position,sym_size,thick,title,zvalue
;   symbol uses a lookup table to equiv
;
; So far, cannot handle 'clip' value as not sure how to define, x|y|ztick_get as pts to a named variable
;
; Did not yet set the xyzVARS because there are so many of them: 3*17, of which almost none convey to objects
;   This means the non-Jupyter 'passthru' does not passthru them to the older plot procedure if used
;
; object plot does not have the equivalent of '/polar'

PRO jnplot,x,y,$
           isotropic=isotropic,max_value=max_value,min_value=min_value, $
           nsum=nsum,polar=polar,thick=thick,data=data,device=device,normal=normal,noclip=noclip, $
           noerase=noerase,t3d=t3d,xlog=xlog,ylog=ylog,ynozero=ynozero,background=background, $
           charsize=charsize,charthick=charthick,clip=clip,color=color,font=font,linestyle=linestyle,$
           position=position,psym=psym,subtitle=subtitle,symsize=symsize,ticklen=ticklen,zvalue=zvalue,$
           title=title, overplot=overplot,$
           xcharsize=xcharsize,xgridstyle=xgridstyle,xmargin=xmargin,xminor=xminor,xrange=xrange,$
           xstyle=xstyle,xthick=xthick,xtick_get=xtick_get,xtickformat=xtickformat,$
           xtickinterval=xtickinterval,xticklayout=xticklayout,xticklen=xticklen,xtickname=xtickname,$
           xticks=xticks,xtickunits=xtickunits,xtickv=xtickv,xtitle=xtitle,$
           ycharsize=ycharsize,ygridstyle=ygridstyle,ymargin=ymargin,yminor=yminor,yrange=yrange,$
           ystyle=ystyle,ythick=ythick,ytick_get=ytick_get,ytickformat=ytickformat,$
           ytickinterval=ytickinterval,yticklayout=yticklayout,yticklen=yticklen,ytickname=ytickname,$
           yticks=yticks,ytickunits=ytickunits,ytickv=ytickv,ytitle=ytitle,$
           zcharsize=zcharsize,zgridstyle=zgridstyle,zmargin=zmargin,zminor=zminor,zrange=zrange,$
           zstyle=zstyle,zthick=zthick,ztick_get=ztick_get,ztickformat=ztickformat,$
           ztickinterval=ztickinterval,zticklayout=zticklayout,zticklen=zticklen,ztickname=ztickname,$
           zticks=zticks,ztickunits=ztickunits,ztickv=ztickv,ztitle=ztitle
           

    COMMON JNSSW, jn_plot, jn_tv

    f=getenv('SSWJUPYTER')
    if f eq '' then SSWFLAG=0 else SSWFLAG=1

    xyvar = 1
    if not keyword_set(y) then begin
        ; due to plot,x,y or plot,y both being acceptable usages
        xyvar=0
        y=x
    end
    if not keyword_set(isotropic) then isotropic=0
    if not keyword_set(max_value) then max_value=max(y)
    if not keyword_set(min_value) then min_value=min(y)
    if not keyword_set(nsum) then nsum=0
    if not keyword_set(polar) then polar=0
    if not keyword_set(thick) then thick=1.0
    if not keyword_set(xlog) then xlog=0
    if not keyword_set(ylog) then ylog=0
    if not keyword_set(ynozero) then ynozero=0
    if not keyword_set(title) then title='  '
    
    if not keyword_set(data) then data=0
    if not keyword_set(device) then device=0
    if not keyword_set(normal) then normal=0
    if not keyword_set(noclip) then noclip=0
    if not keyword_set(nodata) then nodata=0
    if not keyword_set(noerase) then noerase=0
    if not keyword_set(t3d) then t3d=0
    
    if not keyword_set(background) then background=0
    if not keyword_set(charsize) then charsize=1.0
    if not keyword_set(charthick) then charthick=1
    ;if not keyword_set(clip) then clip=??? ; not sure how to define
    if not keyword_set(color) then color='white'
    if not keyword_set(font) then font=!P.FONT
    if not keyword_set(linestyle) then linestyle=0
    if not keyword_set(position) then position=[0,0]
    if not keyword_set(psym) then psym=0
    if not keyword_set(subtitle) then subtitle=''
    if not keyword_set(symsize) then symsize=1.0
    if not keyword_set(ticklen) then ticklen=0.02
    if not keyword_set(zvalue) then zvalue=0.0
    
    symtable=['None','+','dot','D','tu','s','X','','']
    ; SYMBOL/PSYM lookup:
    ;    old PSYM: 1=+ 2=* 3=. 4=diamond 5=triangle 6=square 7=X 8=userdefined 9=undef 1 0=histogram mode
    ;    new: "None", "+", "dot", "D", "tu", "s", "X", 8 = pass thru
   
    ; this bit lets jnplot work for both plot and oplot
    if not keyword_set(overplot) then overplot=0

    ; need to handle parameters, keyword=parameters, and /keywords
    ;print,'Overwriting plot'
    ;print,'sample flag /isotropic: ',isotropic
    ;print,'sample var min_value: ',min_value

    ; BUGS TO FIX:
    ; symbol=symtable[psym] Attempt to call undefined procedure: 'SYMBOL_CONVERT'.
    ; color=color  Attempt to call undefined procedure: 'STYLE_CONVERT'.
    ; title=title  Attempt to call undefined procedure: 'ITEXT'.

    if SSWFLAG then begin
        ; reroutes older plot to newer object oriented plot
        if xyvar then begin
            jn_plot=plot(x,y,overplot=overplot,position=position,thick=thick, $
                max_value=max_value,min_value=min_value,linestyle=linestyle,sym_size=symsize)
        end else begin
            jn_plot=plot(y,overplot=overplot,position=position,thick=thick, $
                max_value=max_value,min_value=min_value,linestyle=linestyle,sym_size=symsize)

        end
    end else begin
        ; passthru to older plot, legacy compatibility
        if overplot then begin
            if xyvar then begin
                oplot,x,y,$
                    polar=polar,max_value=max_value,min_value=min_value,nsum=nsum,thick=thick,noclip=noclip,$
                    t3d=t3d,color=color,linestyle=linestyle,psym=psym,symsize=symsize,zvalue=zvalue
            end else begin
                oplot,y,$
                    polar=polar,max_value=max_value,min_value=min_value,nsum=nsum,thick=thick,noclip=noclip,$
                    t3d=t3d,color=color,linestyle=linestyle,psym=psym,symsize=symsize,zvalue=zvalue
            end
        end else begin 
            if xyvar then begin
                plot,x,y,$
                    isotropic=isotropic,polar=polar,xlog=xlog,ylog=ylog,ynozero=ynozero,$
                    max_value=max_value,min_value=min_value,nsum=nsum,thick=thick,data=data,device=device,$
                    normal=normal,noclip=noclip,nodata=nodata,noerase=noerase,t3d=t3d,title=title,$
                    background=background,charsize=charsize,charthick=charthick,color=color,font=font,$
                    linestyle=linestyle,position=position,psym=psym,subtitle=subtitle,symsize=symsize,$
                    ticklen=ticklen,zvalue=zvalue
            end else begin
                plot,y,$
                    isotropic=isotropic,polar=polar,xlog=xlog,ylog=ylog,ynozero=ynozero,$
                    max_value=max_value,min_value=min_value,nsum=nsum,thick=thick,data=data,device=device,$
                    normal=normal,noclip=noclip,nodata=nodata,noerase=noerase,t3d=t3d,title=title,$
                    background=background,charsize=charsize,charthick=charthick,color=color,font=font,$
                    linestyle=linestyle,position=position,psym=psym,subtitle=subtitle,symsize=symsize,$
                    ticklen=ticklen,zvalue=zvalue
            end
        end
    end
  
END
