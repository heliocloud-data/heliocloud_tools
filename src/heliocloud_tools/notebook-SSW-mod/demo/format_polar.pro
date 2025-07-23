FUNCTION format_polar, img1,center, pmask=pmask, trim=trim, a0=a0, norotate=norotate
  ; returns polar representation, with sun to right of image

  if not keyword_set(trim) then trim=0
  if not keyword_set(a0) then a0=45 ; Antunes, placed so the pylon is not blocking
  if keyword_set(norotate) then a0=0; overwrites the above a0
  print,'rotating ',a0
  ; Antunes, variables needed for Cor2 polar projection
  stepr=1
  ;stepa=1
  stepa=1/2.0 ; Antunes, not sure why 2x works but 1x did not
  nsp=360*2 ; Antunes, ditto
  ;nsp=360
  ; 530 bins = 14RSun (not analytically, just by inspection from rect2pol)
  ; by default rect2pol makes 727 bins/rows, which includes corners
  outer_limit = 530             ; by inspection, for COR2 data

  img_full = rect2pol(img1,center[0],center[1],nsp,a0,stepa,stepr,sp_ave,sp_std)
                                ; Antunes, filter our corners
  if trim then img_full=img_full[*,0:outer_limit]     ; remove corners
  img_full = rotate(img_full,1); puts sun to the right

  ; optionally artifically remove that blob
  ; e.g. pmask=[400,500,20,200] in polar space
  if keyword_set(pmask) then img_full[pmask[0]:pmask[1],pmask[2]:pmask[3]]=0

  return,img_full
END
