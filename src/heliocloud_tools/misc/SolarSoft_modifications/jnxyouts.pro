; usage: replace all xyouts,... with jnxyouts,...
; There are around 7000 plot/oplot/xyouts/set_plot/tvscl/tv
; see also ssw/gen/idl/display and its tv2.plro, xyouts2.pro, etc
; Note plot alone has 42 possible keywords

; Possibility-- rewrite coyote's cgplot for our use:
; http://www.idlcoyote.com/graphics_tips/coyote_graphics.php


PRO jnxyouts,x,y,labels,$
             alignment=alignment, charsize=charsize, charthick=charthick, $
             text_axes=text_axes, width=width, clip=clip, color=color, font=font, orientation=orientation, z=z

    COMMON JNSSW jn_plot, jn_tv

    f=getenv('SSWJUPYTER')
    if f eq '' then SSWFLAG=0 else SSWFLAG=1

    if not keyword_set(alignment) then alignment=0.0
    if not keyword_set(charsize) then charsize=1.0
    if not keyword_set(charthick) then charthick=1.0
    if not keyword_set(text_axes) then text_axes=0
    ;if not keyword_set(width) then width=NAMED VARIABLE??; skip for now
    ;if not keyword_set(clip) then clip=??? ; skip for now
    if not keyword_set(color) then color=!P.COLOR
    if not keyword_set(data) then data=0
    if not keyword_set(device) then device=0
    if not keyword_set(normal) then normal=0
    if not keyword_set(noclip) then noclip=0
    if not keyword_set(t3d) then t3d=0
    if not keyword_set(font) then font=!P.FONT
    if not keyword_set(orientation) then orientation=0
    ;if not keyword_set(z) then z=??? ; skip for now, only applies to t3d

    if SSWJUPYTER then begin
        ; new rerouting to object graphics
        baselines = [ [1.0,0,0], [0,1.0,0], [0,0,1.0], [-1.0,0,0], [0,-1.0,0], [0,0,-1.0] ]
        ;schema: text_axes 0=xy,1=xz,2=yz,3=yx,4=zx,5=zy
        ;        baseline = [1.0,0,0]=xy, default  [0,1.0,0]=parallel to +y [0,0,1.0]=parallel to +z
        axes = baselines(text_axes*3:text_axes*3+2)
        jn_plot = TEXT(x, y, labels, color=color, orientation=orientation, baseline=axes, $
            data=data,device=device,normal=normal)
    end else begin
        ; passthru to original xyouts
        xyouts,x,y,labels,alignment=alignment,charsize=charsize,charthick=charthick,text_axes_text_axes,$
        color=color,data=data,device=device,normal=normal,noclip=noclip,t3d=t3d,font=font,$
        orientation=orientation
    end
END
