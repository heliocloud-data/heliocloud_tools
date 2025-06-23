; usage: replace all tv,... with jntv,asda,das,das,dsas
; and tvscl,... with jntv,/scale,...
;(where common block jn_tv is persistent and used in future plots)
; There are around 7000 plot/oplot/xyouts/set_plot/tvscl/tv
; see also ssw/gen/idl/display and its tv2.plro, xyouts2.pro, etc
; Note plot alone has 42 possible keywords

; Later possibility-- rewrite coyote's cgplot for our use:
; http://www.idlcoyote.com/graphics_tips/coyote_graphics.php

; tv and tvscl takes an image and either optional 'position' index or x,y pair

; tv core parameters: iamge, [x,y]|position
; optional values: top,true,xsize,ysize
; optional flags: centimeters,inches,nan,order,words
; optional graphics values: z,channel
; optional graphics keywords: data,device,normal,t3d
; image equivalents:
; image_location=[0,0] or [x,y]
; image variables not in play: dimensions, layout, location, margin, geotiff, image_dimensions,
;     buffer, current, device, no_toolbar, nodata, overplot, widgets, irregular, ordercd


PRO jntv,imdata, posx, posy, $
            centimeters=centimeters, inches=inches, nan=nan, $
            words=words, order=order, $
            top=top, true=true, xsize=xsize,ysize=ysize, scale=scale

    COMMON JNSSW, jn_plot, jn_tv

    f=getenv('SSWJUPYTER')
    if f eq '' then SSWFLAG=0 else SSWFLAG=1

    xyvar=0 ;0 = none, 1 = x,y, 2 = position
    if keyword_set(posx) then begin
        if keyword_set(posy) then begin
            xyvar=1
        end else begin
            position=posx
            xyvar=2
        end
    end
  
    if not keyword_set(top) then top=!D.TABLE_SIZE-1
    if not keyword_set(centimeters) then centimeters=0
    if not keyword_set(inches) then inches=0
    if not keyword_set(nan) then nan=0
    if not keyword_set(words) then words=0
    if not keyword_set(order) then order=0
    if not keyword_set(true) then true=0
    ;if not keyword_set(xsize) then xsize=??? ; ignore for now, mostly for postscript
    ;if not keyword_set(ysize) then ysize=???; ignore for now, mostly for postscript
    if not keyword_set(data) then data=0
    if not keyword_set(device) then device=0
    if not keyword_set(normal) then normal=0
    if not keyword_set(td3) then td3=0
    if not keyword_set(channel) then channel=0
    
    if not keyword_set(scale) then scale=0 ; /scale or scale=1 calls tvscl-equiv instead of tv

    if not keyword_set(centimeters) then centimeters=0
    if not keyword_set(inches) then inches=0
    ; ignore xsize and ysize, not useful for notebooks unfortunately

    if SSWFLAG then begin

        scImage=imdata
        if scale then begin
            scImage=BytScl(scImage,TOP=!D.N_Colors-1)
        end
    
        ; need to handle parameters, keyword=parameters, and /keywords
        if xyvar eq 1 then begin
            jn_tv=image(scImage,image_location=[x,y])
        end else begin
            jn_tv=image(scImage)
        end
    end else begin
        ; passthru to original tv or tvscl
        if scale then begin
            if xyvar eq 1 then begin
                tvscl,image,posx,posy,$
                    centimeters=centimeters,inches=inches,nan=nan,order=order,words=words,$
                    data=data,device=device,normal=normal,t3d=t3d,channel=channel
            end else if xyvar eq 2 then begin
                tvscl,image,position,$
                    centimeters=centimeters,inches=inches,nan=nan,order=order,words=words,$
                    data=data,device=device,normal=normal,t3d=t3d,channel=channel
            end else begin
                tvscl,image,$
                    centimeters=centimeters,inches=inches,nan=nan,order=order,words=words,$
                    data=data,device=device,normal=normal,t3d=t3d,channel=channel
            end
        end else begin
            if xyvar then begin
                tv,image,position,posy
            end else if position then begin
                tv,image,position
            end else begin
                tv,image
            end
        end
    end
  
END
