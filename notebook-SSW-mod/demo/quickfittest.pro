; in theory should be identical to
;cor2proc_wmodel,date='test',sc='A',/display,minradii=400,/polar,/firstorder,/mpfit

;;; Note the 2 primary routines:
;;; 'quickfittest' is a pre- and post-processor, while
;;; 'quickfitting' is the actual algorithm



;;; core functions
; Be sure to update starting coefficients in 'quickfittest' near this comment:
  ; Initial coefficients for myfunc_cor2
  ;;
  ; below taken from a sample Matlab fit
  
  ;Bee = [70.9611, 2.0265, 1.0917, -22.2257] ; for myfunc_cor2_meetvar
  ;;Bee = [1.0e14, 1.0, 0.3, 200.0, 1.0, 1.0]; for myfunc_gauss2


; same as cor2proc_wmodel
pro myfunc_cor2_orig, X, A, F, returnparams=returnparams
  ; original function, from paper
  if keyword_set(returnparams) then begin
     A=[0.5, 0.5, 1.0, 1.0,1.0,1.0]
  end

   F = A[2] / (A[0] + A[1]*X^A[3])
end

;pro myfunc_cor2_meeting, X, A, F, returnparams=returnparams
pro myfunc_cor2, X, A, F, returnparams=returnparams
  ; new equation, from meeting
  if keyword_set(returnparams) then begin
     A=[70.9611, 2.0265, 1.0917, -22.2257, -17.9646]
     return
  end

  if A[3] le 0 then A[3] = 0-A[3]
  ;if A[4] le 0 then A[4] = 0-A[4]
  F = (A[0] * EXP(A[1]* (X^A[2]))) + A[3] + A[4]*X
end

pro myfunc_cor2_meetvar, X, A, F, returnparams=returnparams
;pro myfunc_cor2, X, A, F, returnparams=returnparams
  if keyword_set(returnparams) then begin
     A=[70.9611, 2.0265, 1.0917, -22.2257]
     return
  end

  if A[3] le 0 then A[3] = 0-A[3]
;;   F = (A[0] * EXP(A[1]* (X^A[2]))) + A[3] + A[4]*X
   F = (A[0] * EXP(A[1]* (X^A[2]))) + A[3]

end


pro myfunc_gauss2, X, A, F, returnparams=returnparams
   ; copy of IDL gauss2 equation
  if keyword_set(returnparams) then begin
     A=[1.0e14, 1.0, 0.3, 200.0, 1.0, 1.0]
  end

   F = A[0] * exp(-((X-A[1])/A[2])^2.0) + A[3] * exp(-((X-A[4])/A[5])^2)

end


;;; little plotting stubs once 'purefit.sav' is made

pro plot_bkg_sub

  restore,/v,'purefit.sav'
  window,xsize=1024,ysize=1024,/free
  tvscl,rebin(imgin,512,512),0,512,/device  
  tvscl,rebin(imgfit3,512,512),512,512,/device      
  tvscl,rebin(imgin-imgfit3,512,512),0,0,/device    
  tvscl,rebin(alog(imgin-imgfit3),512,512),512,0,/device

  xyouts,250,1000,'Data 20120307_01240',/device, charsize=3
  xyouts,750,1000,'Bkgd Fit 20120307_01240',/device, charsize=3
  xyouts,250,20,'Data-Bckg',/device,charsize=3 
  xyouts,750,20,'log(Data-Bckg)',/device,charsize=3 
end

pro plot_wispr
  restore,/v,'purefit.sav'
  window,/free,xsize=1500,ysize=1500
  tvscl,rebin(imgin,512,512),20,720,/device
  tvscl,rebin(imgfit3,512,512),720,720,/device
  sub=imgin-imgfit3
  tvscl,rebin(sub>0,512,512),400,20,/device
end

PRO show_wispr
  ; hacked up plot4_display for wispr trios
  restore,/v,'purefit.sav'
;;;  window,/free,xsize=1500,ysize=1500
  window,/free,xsize=1500,ysize=800
  ;imgquads=[ [0,750], [750,750], [0,0], [750,0]]
  ;labelquads=[ [360,1400], [900,1400], [360,600], [900,600]]

  xyouts,560,620,file_basename(file),charsize=3,/device
  
  ;;;tvscl,rebin(imgin > 0 < 25,512,512),0,750,/device
  tvscl,rebin(imgin > 0 < 25,512,512),0,50,/device
  ;;;xyouts,100,1400,'WISPR L2 DATA no bkgd-sub',charsize=3,/device
  xyouts,50,650,'WISPR L2 DATA no bkgd-sub',charsize=3,/device
  ;;;tvscl,rebin(imgfit3 > 0 < 25,512,512),750,750,/device
  ;;;xyouts,800,1400,'F-corona fit-generated MODEL_BKGD',charsize=3,/device
  tvscl,rebin( (imgin-imgfit3) > 0 < 25 ,512,512),500,50,/device
  xyouts,580,650,'DATA - MODEL_BKGD',charsize=3,/device

  pause

  restore,/v,'sampleslice.sav'
  plot,x,y
  restore,/v,'sampleslicefit.sav'
  oplot,x,yfit1
  xyouts,360,700,'sample radial slice and fit',charsize=2,/device

END


PRO dump_to_matlab,x,y,fname
  OPENW,1,fname
  PRINTF,x,y,FORMAT='F12.5,1x,F12.5'
  CLOSE,1
END




;;; misc functions


PRO wscrub,image,mymin,mymax
  ;image = image > mymin < mymax
END


FUNCTION fcorona_better,none=none
  coord = [511.0, 510.0, 7.21, 130.6/2.0]
  sundist,coord,dist,angle,xsize=1024,ysize=1024

  pangle = angle
  ; if it is -2.47 at eq and -2.25 at pol
  ; polar angles 0-360 map as:
  ;  0-180: lat = 90 - polar.  180-360: lat = polar - 270

  idx = where (angle le 180.0)
  pangle[idx] = 90.0 - angle[idx]
  idy = where (pangle gt 180.0)
  pangle[idy] = angle[idy] - 270.0
  ;pangle=abs(pangle) ; no diff between +polar and -polar

  coef = 3.3e-8; F-corona MSB at 1Rsun

  expon = 0.22 * cos(pangle*!dtor)-2.47; array of coeffs
  fcorona = coef * dist^expon
  zero_outer_circle,fcorona,center=[coord(0),coord(1)],minradii=100
  ;fcorona=fcor_kl(dist,pangle)
  return,fcorona

END

FUNCTION fcorona_alt,none=none
  ;fov = 15 Rsun radius for 1024 pixels aka 1024=30 Rsun
  fov = 30.0/1024.0
  ctr = [510.0,511.0]
  rollangle = 7.21*!dtor
  ;coord = [511.0, 510.0, 7.21*!dtor,fov ] ;    ;, 130.6/2.0]
  ; coord = [col center, row center, roll angle of north, radius]
  ;sundist,coord,dist,angle,xsize=1024,ysize=1024
  ; weirdness in 'sundist' on angles has me worried (it is asymmetric!)

  row=(LINDGEN(1024) - ctr[0]) * fov
  col=(LINDGEN(1024) - ctr[1]) * fov
  rows=fltarr(1024,1024)
  for i=0,1023 do rows[*,i]=row
  cols=fltarr(1024,1024)
  for i=0,1023 do cols[i,*]=col
  dist = sqrt( (rows*rows) + (cols*cols))

  latitude = ATAN (abs(cols),abs(rows)) - rollangle ; is in radians

  coef = 3.3e-8; F-corona MSB at 1Rsun

  expon = 0.22 * cos(latitude)-2.47; array of coeffs
  fcorona = coef * dist^expon
  zero_outer_circle,fcorona,center=[ctr(0),ctr(1)],minradii=100
  ;fcorona=fcor_kl(dist,pangle)
  return,fcorona

END

FUNCTION f_gauss,inputimg,radii=radii

  if not keyword_set(radii) then radii=100

  myimg=inputimg
  ; fit a given image to an ellipse, then compare
  ; ZONE or MASK?
  zero_outer_circle,myimg,minradii=radii,outerfilter=0.99
  ; crucial-- mask out regions to better this fit
  mask = floor(myimg*0) + 1
  big = where (myimg gt 4000)
  low = where (myimg lt 1)
  mask[big]=0 ; ignore brightness
  mask[low]=0 ; ignore coronograph and other zeroes
  ; remove pylon too, from 512,512 to 750, 1024
  mask[512:700,550:900]=0
  mask[600:800,650:1000]=0
  mask[800:900,800:1000]=0
  ; and that glow patch
  mask[430:512,550:700]=0

  ; FIT
  myfit = gauss2dfit(myimg,params,mask=mask,/tilt)
  ; MASK
  idx=where (mask eq 0)
  myimg[idx]=0
  myfit[idx]=0

  ;zero_outer_circle,myfit,minradii=radii,outerfilter=0.99
  ; RESCALE
  myfit2=myfit*max(myimg*mask)/max(myfit*mask)
  ; COMPARE
  diff = abs(myimg-myfit)
  diff2 = abs(myimg-myfit2)
  match=myimg
  idx = where (diff gt 0.80*myimg) ; area where both are not within X%
  match[idx]=0.0
  plot4_display,myimg*mask,0,'Data',/init
  plot4_display,myfit*mask,1,'Ellipse Fit'
  plot4_display,diff*mask,2,'delta'
  ;plot4_display,match*mask,3,'Matched within X%'
  plot4_display,diff2*mask,3,'scaled delta'


  ; see also 'show3,imgfit3 < 4000' and 'contour,imgfit3 < 4000'

  return,myfit

END

PRO event2012

  ; L0 data
  f1='cor2_misc/2012-cme/20120307_012400_d4c2A.fts'
  f2='cor2_misc/2012-cme/20120307_013900_d4c2A.fts'
  f3='cor2_misc/2012-cme/20120307_015400_d4c2A.fts'
  f4='cor2_misc/2012-cme/20120307_023900_d4c2A.fts'
  f5='cor2_misc/2012-cme/20120307_025400_d4c2A.fts'

  ; should probably run cor2_prep on them

  b1='cor2_misc/2012-cme/mc2A_pTBr_120311_monthlyminBkg.fts'
  bkg=float(sccreadfits(b1,bhdr))

  ; raw compare for running diff
  files = [f1,f2,f3,f4,f5]
  cube=float(sccreadfits(files,hdrs))  ; WHY INT RETURN?????????
  cube2=rebin(cube,1024,1024,5)

  ; gorgeous CME!
  ;;tvscl,cube2[*,*,0]-cube2[*,*,1]

  quickfittest,/alt2048
  restore,/v,'purefit.sav'

  im2=cube2[*,*,2]
  hdr2=hdrs[2]
  im1=cube2[*,*,1]
  ;;zero_outer_circle,im2,center=center,minradii=100 ,outerfilter=0.99
  ;;zero_outer_circle,im1,center=center,minradii=100 ,outerfilter=0.99

  fakeback=(imgfit3*5.0)+2000.0  ; why this scaling needed?
  diffimg = im2-im1
  pleasework=im2-fakeback

  please2=pleasework
  zero_outer_circle,please2,center=[1016.43/2.0,1023.62/2.0],minradii=130 ,outerfilter=0.99
  please2[155:190,690:710]=0

  fxread,f2,data2,hdr2b ; re-read because I need the fx hdr, not the scc hdr
  data2=float(data2)
  ;etas = -90 to +90
  ;;;img_kl=fcorona(data2,hdr2b,fjustf)
  ;;;fjustf=float(fjustf)
  fjustf=fcorona_alt()

  ; InvDiff aka creating a background using different-image concepts
  ; diffimg is not a background but a feature-enhancer, so the
  ; actual diff background would be
  ; im2 - im1 => + is new in im2, - is new in im1
  ; so
  diffrev = im1 - im2
  ind2 = where (diffimg gt 0.) ; location of features new to im2
  ind1 = where (diffrev gt 0.) ; location of features new to im1
  qtemp2 = im2 ; start with im2
  qtemp1 = im1 ; start with im2
  qtemp2[ind2] = im1[ind2] ; replace im2 'features' with im1 stuff
  qtemp1[ind1] = im2[ind1] ; replace im1 'features' with im2 stuff
  invdiff = (qtemp1 + qtemp2) / 2.0 ; average them


  print,'sizes',max(im2),max(imgfit3),max(diffimg),max(im2-imgfit3)
  print,max(invdiff),max(fjustf),max(abs(imgfit3-fjustf))

  ; jam up imgfit3 and fcorimg to max of im2
  m2=max(im2)
  imgfit3 = imgfit3 * (m2/max(imgfit3))
  fjustf = fjustf * (m2/max(fjustf))
  img_kl = m2 - fjustf

  print,'sizes',max(im2),max(imgfit3),max(diffimg),max(im2-imgfit3)
  print,max(invdiff),max(fjustf),max(abs(imgfit3-fjustf)),max(img_kl)

  modeldiffs = imgfit3-fjustf

  plot4_display,im2 > 0,0,'L0 20120307 CME',/init
  ;plot4_display,fakeback > 0,1,'Model-fit background from L0 data'
  plot4_display,imgfit3 > 0,1,'Model-fit background from L0 data'
  plot4_display,diffimg > 0,2,'L0 diff image'
  ;plot4_display,please2 > 0,3,'L0-Model_fit_bck'
  plot4_display,im2-imgfit3 > 0,3,'L0-Model_fit_bck'

  plot4_display,invdiff > 0,0,'Invdiff background',/init
  plot4_display,fjustf > 0,1,'KLmodel alone'
  plot4_display,img_kl > 0,2,'L0 - KLmodel'
  plot4_display,abs(imgfit3-fjustf) > 0,3,'KLmodel - Model-fit'

  ; im2 = L0 image
  ; img_kl = L0 - KL F-corona
  ; fjustf = just the KL F-corona
  ; imgfit3 = just my F-corona
  ; im1 = useful to make a diff image
  ; invdiff = 'inverted diff' aka diff = 'features' -> im2-diff = 'background'
  save,file='temp.sav',im2,img_kl,imgfit3,im1,invdiff,fjustf,hdr2,hdr2b

END

FUNCTION colsmooth,poldata

  ; walk along each column and smooth across 14 bins
  ; (typically 720 columns so about a 2% smooth boxcar)
  newpol = poldata
  ncols = (size(newpol,/dim))[0]
  for t=1,ncols-2 do begin
     newpol[t,*]=smooth(poldata[t,*],14,/edge_truncate)
     ; newpol[t,*]=gauss_smooth(y,/edge_wrap)
  end
  return,newpol
END

FUNCTION rowsmooth,poldata

  ; walk along each row and smooth across nsmooth bins as a 2% smooth boxcar
  newpol = poldata
  nrows = (size(newpol,/dim))[1]
  nsmooth = nrows*0.02
  if nsmooth lt 9 then nsmooth = 9 ; enforce at minimum boxcar
  for t=1,nrows-2 do begin
     newpol[*,t]=smooth(poldata[*,t],nsmooth,/edge_truncate)
     ; newpol[t,*]=gauss_smooth(y,/edge_wrap)
  end
  return,newpol
END


FUNCTION limitscreen,mydata,mymin,mymax
  ; weeds out outliers from given limits
  ; sets items outside 120% of limits to be 120% of limits
  temp=mydata
  ii = where (mydata lt 1.2*mymin)
  temp[ii]=1.2*mymin
  ii = where (mydata gt 1.2*mymax)
  temp[ii]=1.2*mymax
  return,temp
END


;;; the actual fitting
;;; 'quickfittest' is a pre- and post-processor, while
;;; 'quickfitting' is the actual algorithm



FUNCTION quickfitting, myimg,diag
  ; now with cool movie-like fitting with /diag flag on

  if not keyword_set(diag) then diag=0
  print,'fitting image with size:',size(myimg)
  nRows  = (size(myimg))[2] ; 1 = r, 2 = theta

  ; Initial coefficients for myfunc_cor2
  ;mybaseline = median(myimg)
  ;;Bee = [0.5, 0.5, 1.0, 1.0,1.0,1.0] ; for myfunc_cor2_orig (not working!)
  ; below taken from a sample Matlab fit
  ;Bee = [70.9611, 2.0265, 1.0917, -22.2257, -17.9646] ; for myfunc_cor2_meeting
  ;Bee = [70.9611, 2.0265, 1.0917, -22.2257] ; for myfunc_cor2_meetvar
  ;;Bee = [1.0e14, 1.0, 0.3, 200.0, 1.0, 1.0]; for myfunc_gauss2

  ; fetch initial conditions using a mod/hack of the fitting function
  Bee=1
  myfunc_cor2,0,Bee,0,/returnparams ; handler to initialize these
  print,'Starting coefficients are ',Bee

  Beekeep=Bee

  badfitscounter1 = 0

  model=myimg*0

  ylimit=max(myimg) ; for plotting use later
  ylow=min(myimg)   ; for diagnostic use

  label = 'data slice along a radial line, radial dist vs brightness'

  ; save all fit params for later
  saveFitParams = make_array( 2+(size(Bee,/dim))[0], nRows, /float)

  FOR t = 0, nRows-1 DO BEGIN
  ;FOR t = 515,528 DO BEGIN
     y = myimg[*,t] ; radial slice, i.e. for y=theta, all x values
     szy=(size(y))[1]
     ;print,'size',szy,nRows
     x = findgen(szy)/szy 
     wts = 1/y

     ;if t eq 100 then save,file='sampleslice.sav',t,x,y,label,ylimit

     ; DATA CLEANING TO HELP FITTING
     valid = where(y gt 0)
     ; actually fits are good keeping in the 'dives', I think because
     ; it counter-balances the edge peaks before we hit there, in effect
     ; de-emphasizing strong edge effects.  Worth looking into.
     ; So maybe keep this, maybe not?
     ;print,size(valid)
     ; if-check for when an entire row is 0 or bad
     if size(valid,/dim) gt 5 then $
        valid=valid[2:-3]  ; truncate endpt dives to 0, which messes up fitting

     if diag then plotslice,t,x[valid],y[valid],label=label,ylimit=ylimit; panic! at the plotting

     ; found weirdness-- sometimes it fits better with initial params,
     ; sometimes with inherited params.
     ; works with myfunc_cor2

     yfit1 = mpcurvefit(x[valid], y[valid], wts[valid], Bee, $
                        function_name='myfunc_cor2', /noder, itmax=10000, $
                        /double,status=status,/quiet,errmsg=errmsg) ; tol=tol

     yfit2=y*0
     yfit2[valid]=yfit1
     yfit1=yfit2

     ;save,t,x,y,yfit1,valid,Bee,wts,file='slice'+strtrim(t,2)+'.sav'

     if status lt 1 then begin
        Bee = Beekeep           ; if fails, revert params and try a refit
        yfit1[valid] = mpcurvefit(x[valid], y[valid], wts[valid], Bee, $
                          function_name='myfunc_cor2', /noder, itmax=10000, $
                          /double,status=status,/quiet,errmsg=errmsg) ; tol=tol
        ; if that also fails, give up
        if status lt 1 then begin
           print,'(Re-trying row '+strtrim(t,2)+' due to bad initial fit... fit failed)'
           ++badfitscounter1
           Bee = Beekeep
        end else begin
           ; hey, the refit with reset parameters worked, keep it
           print,'(Re-trying row '+strtrim(t,2)+' due to bad initial fit... fit succeeded!)'
        end
     end

     saveFitParams[*,t] = [t,t,Bee]

     if t mod 100 eq 0 then print,'done fit 1 for row '+strtrim(t,2)+', status='+strtrim(status,2),errmsg,' Fit coefficients:'
     if t mod 100 eq 0 then print,Bee


     ; Here is why we should get rid of the convolution for Cor2
     ;;;h = savgol(55,55,0,2)
     ;;;f=y*0
     ;;;f[valid] = convol(y[valid]/yfit1[valid],h,/edge_t,/nan,missing=0)
     ;;;newF = yfit1 * f  ; newF --> I[1]

     ;;;if diag then plotslice,t,x,newF,/overplot,/delay,color='FF0000'x ; panic! at the plotting

     if t mod 100 eq 0 then save,t,x,y,yfit1,label,ylimit,file='slice'+strtrim(t,2)+'.sav'
     if t mod 100 eq 0 then write_csv,'slice'+strtrim(t,2)+'.csv',x[valid],y[valid],yfit1[valid]

     if diag then plotslice,t,x[valid],yfit1[valid],/overplot,/delay ; panic! at the plotting

     if max(yfit1) gt ylimit*1.2 or min(yfit1) lt ylow then print,'possible bad fit for row '+strtrim(t,2)+', max/min are '+strtrim(max(yfit1),2)+','+strtrim(min(yfit1),2)+' vs data max/min of '+strtrim(ylimit,2)+','+strtrim(ylow,2)

     model[*,t]=yfit1

  ENDFOR

  save,file='fitparams.sav',saveFitParams

  print,'Eq 1 fit '+strtrim(nRows-badfitscounter1,2)+' out of '+strtrim(nRows,2)+' rows'

  RETURN, model

END

PRO comparefit,img,fit
  sz=size(img)
  szy=sz[2]-1
  for j=0,szy,10 do begin
     plot,img[*,j]
     oplot,fit[*,j]
     wait,0.2
  end

END

; ==========================================================================

PRO quickfittest,alt2048=alt2048, wispr=wispr, file=file, $
                 diag=diag, silent=silent, nosave=nosave, minradii=minradii,$
                 polfit3=polfit3, imgfit3=imgfit3,align=align, scale=scale,$
                 norighthalf=norighthalf,norotate=norotate
  ; 'polfit3' is optional return variable
  ;window,/free,xsize=1500,ysize=1500

  if not keyword_set(alt2048) then alt2048=0  ; 1-> use alt data set
  if not keyword_set(wispr) then wispr=0  ; 1-> use wispr data
  if not keyword_set(align) then align=0  ; 1-> align imgs to solar north 1st
  if not keyword_set(diag) then diag=0  ; 1-> also show fit curve plots
  if not keyword_set(minradii) then minradii=0  ; extra occulting
  if not keyword_set(scale) then scale=1.0  ; rescale factor for images
  saveme=1 ; default is to save data as 'purefit.sav'
  if keyword_set(nosave) then saveme=0  ; 0-> do not save as 'purefit.sav'
  showtv=0 ; default is to show before/after tv plots as it fits
  if not keyword_set(silent) then showtv=1  ; 1-> show imgs, 0-> go dark
  if keyword_set(norotate) then norotate=1 else norotate=0
  if keyword_set(norighthalf) then norighthalf=1 else norighthalf=0
  if norighthalf then norotate=1 ; required for proper transforms

  ; this is a background file
  if not keyword_set(file) then $
     file = '/homes/antunak1/sswdb/stereo/secchi/backgrounds/a/monthly_min/test/mc2A_pTBr_090204.fts'
  ; this is a real data file
  ;if alt2048 then file = '/homes/antunak1/cor2_misc/20120602_000815_n4c2A.fts'; TEST2
  ;if alt2048 then file = '/homes/antunak1/cor2_misc/20120602_020815_n4c2A.fts'; TEST2
  ;if alt2048 then file = 'cor2_misc/2012-cme/20120307_013900_d4c2A.fts'
  if wispr then file = getenv('SSW')+'/jnpro/demo/psp_L2_wispr_20190405T091254_V3_2222.fits'
  if wispr then print,file
  ;psp_L2_wispr_20190405T091254_V3_2222.fits'
                                ;if wispr then file = '/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20190405T091254_V3_2222.fits'
;if wispr then file = '/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20181101T013030_V3_2222.fits' ; GOOD
;if wispr then file = '/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20181101T013048_V3_1221.fits'  ; fails (bad data)
;if wispr then file = '/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20181101T034548_V3_1221.fits' ; fails (bad data)
;if wispr then file = '/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20181101T103030_V3_2222.fits'  ; GOOD
;if wispr then file='/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20181110T052948_V3_1221.fits' ;FAILS (bad data)
;if wispr then file ='/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20181110T061430_V3_2222.fits' ;good artifact-y
;if wispr then file ='/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20190405T091203_V3_1221.fits'  ; FAILS
;if wispr then file ='/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20190405T091254_V3_2222.fits';  GOOD
;if wispr then file ='/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20210122T030904_V1_2302.fits' ; GOOD
;if wispr then file ='/homes/antunak1/sswdb/wispr_data/L2/psp_L2_wispr_20210122T030904_V1_2302.fits' ; GOOD


  print,'Ingesting',file

  imgin=scale*float(sccreadfits(file,header)) ; Antunes files mod
  wcs=fitshead2wcs(header)
  center=wcs_get_pixel(wcs,[0,0])

  if align then begin
     ; rotate to align to solar north
     rollangle=wcs.roll_angle
     imgin=rot(imgin,0.0-rollangle)
  end

  ; FLAW, right now this only really handles 1024x1024, make dynamic later
  if alt2048 then begin
     imgin=rebin(imgin,1024,1024)              ; TEST2
     ;imgin = 0.2*(imgin - 2000.0)              ; TEST2
     center=center/2.0                         ; TEST2
  end
  if wispr then begin
     ; original is 1920x2048 so, eh
     temp=imgin*1.0e12
     temp=rebin(temp,1920/2,2048/2)
     imgin=fltarr(1024,1024)
     imgin[64:*,*]=temp  ;imgin[0:959,*]=temp
     imgin=reverse(imgin)
     center=center/2.0                         ; TEST2
     center[0]= 1024- (center[0]+64)
     centershift=32
     ; to move center into image, shift it all over (losing outer edge)
     imgin[centershift-1:*,*]=imgin[0:1024-centershift,*]
     imgin[0:centershift+2,*]=imgin[0:centershift+2,*]*0
     center[0]=center[0]-centershift
  end

  ;;if keyword_set(nolefthalf) then imgin[0:center[0],*]=0
  ;;if keyword_set(norighthalf) then imgin[center[0]:*,*]=0

  ; --- check NaNs
  chkNan = finite(imgin, /nan)
  idNan = where(chkNan eq 1, ct)
  if (ct gt 0) then imgin[idNan] = median(imgin) 

  print,'mydata min/max '+strtrim(min(imgin),2)+'/'+strtrim(max(imgin),2)+$
        ', size=',size(imgin,/DIM)
  print,'center is ',center

  if minradii gt 0 then begin
     zero_outer_circle,imgin,center=center,minradii=minradii ,outerfilter=0.99
  end else if minradii ne 0 then begin
     if not wispr then zero_outer_circle,imgin,center=center,minradii=100 ,outerfilter=0.99
  end
  if wispr then wscrub,imgin,0,25; DEBUG
  if showtv then plot4_display,imgin,0,'Background file',/init
  if showtv then xyouts, 10, 1460, 'data', charsize=2, /device

  if wispr then begin
     ; wispr does not need polar imaging, so just copy it
     polimg=imgin
  end else begin
     polimg=format_polar(imgin,center,trim=1,norotate=norotate)
  end

  ; to show just one side, first need to over-fit so p2r has enough overage
  ; to properly interpolate, so it takes 2 steps.  This is step 1.
  ;if norighthalf and norotate then begin
  ;   polimg[*,560:*] = 0
  ;   polimg[*,0:160] = 0
  ;end

  psz=size(polimg)
  if wispr then wscrub,imgin,0,25; DEBUG
  label='Polar data' ;else label='Subsetted data'
  if showtv then plot4_display,polimg,1,label,polar=polar

  print,'in ',min(imgin),max(imgin),median(imgin),size(imgin)
  polfit = quickfitting(polimg,diag)

  ; repair work on the parameters output to convert to proper polar angles
  restore,/v,'fitparams.sav'
  t = saveFitParams[0,*]

  a0=45 ; default is to start fit from pylon so it interfers less
  if norotate then a0=90
  angles = t720_to_polar(t,a0)
  saveFitParams[1,*]=angles
  write_csv,'fitparams.csv',saveFitParams
  save,file='fitparams.sav',saveFitParams ; updates this
  print,'Parameters saved in fitparams.sav'

  polfit2 = limitscreen(polfit,0.0,max(imgin)) ; removes outliers

  polfit3 = colsmooth(polfit2); new smooth by column

  ;print,'debug: out (raw) ',min(polfit),max(polfit),median(polfit),size(polfit)
  ;print,'debug: out (cleaned) ',min(polfit2),max(polfit2),median(polfit2),size(polfit2)

  if wispr then wscrub,imgin,0,25; DEBUG
  label='Polar fit'
  if showtv then plot4_display,polfit2,2,label,polar=polar

  if wispr then begin
     ; wispr does not need polar imaging, so just copy it
     imgfit2=polfit2
     imgfit3=polfit3
  end else begin
     imgfit2=unformat_polar(polfit2,center,norotate=norotate)
     imgfit3=unformat_polar(polfit3,center,norotate=norotate)
  end

  ; to show just one side, first need to over-fit so p2r has enough overage
  ; to properly interpolate, so it takes 2 steps.  This is step 1.
  if norighthalf and norotate then begin
     imgfit2[513:*,*]=0
     imgfit3[513:*,*]=0
  end

  if wispr then wscrub,imgin,0,25; DEBUG
  if showtv then plot4_display,imgfit3,3,'Model fit (smoothed)'

  if saveme then begin
     save,imgin,polimg,polfit,polfit2,polfit3,imgfit2,imgfit3,file,file='purefit.sav'
     print,"Saved as purefit.sav"
  end

END





