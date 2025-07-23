PRO plot4_display,dummy,quadrant,titlestr,polar=polar,init=init

  if keyword_set(init) then window,/free,xsize=1500,ysize=1500

  imgquads=[ [0,750], [750,750], [0,0], [750,0]]
  labelquads=[ [360,1400], [900,1400], [360,600], [900,600]]

  loc=imgquads[*,quadrant]
  pts=labelquads[*,quadrant]
  plotimg1=borderme(dummy)
  nani = where(~(FINITE(plotimg1)))   ; removing NaN
  plotimg1[nani]=0                    ; removing NaN

  if not keyword_set(polar) then begin
     ; only rebin if it won't fail
     sz=size(plotimg1)
     if (sz[1] MOD (sz[1]/2) eq 0) and (sz[2] MOD (sz[2]/2) eq 0) then begin
        plotimg1=rebin(plotimg1,sz[1]/2,sz[2]/2)
     end
  end
  ;print,'plotting, image max is ',max(plotimg1)
  ;plotimg1[0,0]=3500; friday
  tvscl,plotimg1,loc[0],loc[1],/device
  xyouts, pts[0], pts[1], titlestr, charsize=3, /device

END
