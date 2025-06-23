FUNCTION unformat_polar,dummy,center,endsize=endsize,a0=a0,norotate=norotate
  if not keyword_set(endsize) then endsize=1024
  if not keyword_set(a0) then a0=45 ; Antunes, placed so the pylon is not blocking
  if keyword_set(norotate) then a0=0; overwrites the above a0
  ; Antunes, variables needed for Cor2 polar projection
  stepr=1
  ;stepa=1
  stepa=1/2.0 ; Antunes, not sure why 2x works but 1x did not
  nsp=360*2 ; Antunes, ditto
  ;nsp=360
  ; 530 bins = 14RSun (not analytically, just by inspection from rect2pol)
  ; by default rect2pol makes 727 bins/rows, which includes corners
  ;outer_limit = 530             ; by inspection, for COR2 data

  tempdummy = rotate(dummy,3)

  sz=size(tempdummy)
  radius=sz[2]       ; array is 0-360 deg in approp units, then r=0 to whatever

  if a0 ne 0 then begin
  ; back it up 45 deg
     ;; calc based on -45 deg with 720 half-degree bins, 0-719
     ; fwiw, here is +45
     ;dummy2[630:719,*]=dummy[0:89,*]
     ;dummy2[0:629,*]=dummy[90:719,*]
     ; here is -45
     ;dummy2[0:89,*] = dummy[630:719,*]
     ;dummy2[90:719,*] = dummy[0:629,*]
;  if not keyword_set(right) and not keyword_set(left) then begin
     ; for full-frame, rotate back 45 deg

    dummy2=tempdummy*0.
    dummy2[0:89,*] = tempdummy[630:719,*]
    dummy2[90:719,*] = tempdummy[0:629,*]
     ; below dynamic sizing may or may not work for values other than
     ; stepr=1, stepa=1/2.0, nsp=360*2, a0=45, not tested past that case
    tempdummy=dummy2
 end

  phi=[0,360]

  invdummy = p2r(tempdummy,radii=[0,radius],center=center,finalsize=[endsize,endsize],phi=phi,/DEGREES)

  return,invdummy
END
