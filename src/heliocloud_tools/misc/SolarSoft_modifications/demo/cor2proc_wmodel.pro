; usage cor2proc_wmodel,date=date,sc=sc (plus many optional flags/options)
; see full routine below for actual details
; support functions first, main cor2proc_wmodel follows

pro myfunc_cor2, X, A, F
   F = A[2] / (A[0] + A[1]*X^A[3])

   ;F=400*X

   ;F = A[0] + gaussian(x,A[1:4])

   ;inds = where (F>3400)
   ;F[inds]=3400

end

pro myfunc2_cor2, X, A, F

   F = 1.*A[0]*A[1]^(X*A[3]) + A[2]  

   ;;non-physical polynomial just for testing code
   ;F = A[0] + A[1]*X + A[2]*X^A[3]

end


; ================================= MODEL CREATION =====================================

FUNCTION fitting, m1, img0, model = model, test=test, firstorder=firstorder, $
                  mpfit=mpfit,tol=tol,cscale=cscale, $
                  debug=debug, status=status, p1=p1, p2=p2, diag=diag
  ; img nao serve somente para determinar tamanho da imagem, pode ser omitido
  ; model eh a saida, resultado da aplicacao dos 3 passos
  ;m1 eh a entrada a ser trabalhada
  ; img is not only used to determine image size, it can be omitted
  ;  model is the output, result of the application of the 3 steps
  ; m1 is the input to be worked

if keyword_set(test) then begin
    linecolors
    window, 0, xs=1024, ys=1024
    !p.multi=[0,2,2]
endif

if not keyword_set(tol) then tol=1.e-26
if keyword_set(debug) then debug=1 else debug=0

sizeImg0 = size(m1, /dim)
length = sizeImg0[0]
nRows  = sizeImg0[1]

xR = fltarr(length) 

if not keyword_set(p1) then p1=[0.5, 0.5, 1., 1.]  ; p1=[0.3, -0.3, 7., 0.2]; 
if not keyword_set(p2) then p2=[0.005, 2.7, 0.001, 1.]

B = [p1, 1., 1.]
C = p2

amplitude = fltarr(nRows)

; to sync data with fit2 min/max
; eq is C0  * C1 x^C3 + C2
; Note Stenborg/Howard paper uses C0 * C1 x^C2 + C3 as notation
if keyword_set(cscale) then C = C*cscale

badfitscounter1 = 0
badfitscounter2 = 0

;window
if diag then device,decomposed=1             ; needed for color plots  ; thurs
if diag then pause; thurs

FOR t = 0, nRows-1 DO BEGIN
;FOR t = 290, 300 DO BEGIN  ; hack, debug, Antunes, minitest  ; thurs

   ; just to track how it is progressing
   if t mod 100 eq 0 then print, 'row',t

   ;yOri = img0[*,t]

   y = m1[*,t]
   szy=size(y)
   x = findgen(szy[1])/(szy[1]) 

   if debug then $
      if min(y) eq max(y) then print,'degenerate deriv_newF ',mean(y)


; ----- STEP 1:
   if debug then print,'debug: starting fit 1 for row ',t,' median value is ',median(y)

   wts = 1/y
   ; subset data where data is 0/ignorable
   valid = where(y gt 0)

   ; hack for invalid lines i.e. all eq 0
   if valid[0] eq -1 then begin
      ; we do this so the previous good row's params aren't lost
      yfit1=y
      status=1; technically it 'fit' because no fitting needed

   end else begin

      if diag and t mod 100 eq 0 then plot,X[valid],y[valid] ; thurs

      if keyword_set(mpfit) then begin
         yfit1 = mpcurvefit(X[valid], y[valid], wts[valid], B, function_name='myfunc_cor2', /noder, itmax=100, tol=tol,/double,status=status,/quiet)
         ;yfit1 = mpcurvefit(X, y, wts, B, function_name='myfunc_cor2', /noder, itmax=100, tol=tol,/double,status=status,/quiet)
      endif else begin
         yfit1 = curvefit(X[valid], y[valid], wts[valid], B, function_name='myfunc_cor2', /noder, itmax=100, tol=tol,/double,status=status)
      end
      yfit2=y*0
      yfit2[valid]=yfit1
      yfit1=yfit2
   end

   if diag and t mod 100 eq 0 then begin
      xcut=x[valid]
      ycut=y[valid]
      fname='slice_'+strtrim(t,2)+'.sav'
      save,t,x,y,yfit1,xcut,ycut,filename=fname
   end

   if status lt 1 then begin
      statstr = ' did not fit, status' 
      ++badfitscounter1
      B = p1; also revert to generic fit params so bad fit doesn't bias
   endif else begin
      statstr = ' fit status'
   end
   if debug then print,'debug: done fit 1 for row ',t,statstr,status

   if diag and t mod 100 eq 0 then oplot,X,yfit1,color='00FF00'x; thurs
   if diag and t mod 100 eq 0 then pause; thurs

   yfit = yfit1     		;---- POSTER: yfit ---> I[0]
   ; interim save of 1st order model
   model[*,t]=yfit

   ;r = B  

   ; I think the convol magnifies artifacts
   h = savgol(55,55,0,2)
   f = convol(y/yfit,h,/edge_t,/nan,missing=0) 	;---- POSTER: f ---> alpha

   newF = (yfit*f)              ;---- POSTER: newF ---> I[1]

   ; interim save of 1st order model
   ;model[*,t]=sqrt(newF)

   ; fun drawing of fit as it goes, 100 rows at a time
   if t mod 100 eq 0 then plot4_display,model,2,''

   if keyword_set(firstorder) then begin
      continue ; exit this iteration and go to next row
   endif

; ---- STEP 2:

   ;deriv_newF = deriv(smooth(newF,7))      ; 15 too bright
   deriv_newF = deriv(newF)

   ; Braga default: poisson weighting
   ;yfitD = curvefit(X, deriv_newF, 1./Y, C, function_name='myfunc2_cor2', /noder, itmax=150, tol=1.e-26,/double)

   ; Antunes: poisson weighting (Braga's default) with better 'mpcurvefit'
   ; same number of 'distribution too weird' and non-convergence
   ; but mpcurvefit handles them better, see also the url below:
   ;    http://www.idlcoyote.com/math_tips/sigma.html 
   if debug then print,'debug: starting fit 2 for row ',t,' median value is ',median(deriv_newF)

   if min(deriv_newF) eq max(deriv_newF) then print,'degenerate deriv_newF ',mean(deriv_newF)

   ;if t mod 300 eq 0 then save,file='temp.sav',x,deriv_newF,C

   if keyword_set(mpfit) then begin
      yfitD = mpcurvefit(X, deriv_newF, 1./Y, double(C), function_name='myfunc2_cor2', /noder, itmax=150, tol=tol,/double,status=status,/quiet) ; add  ,status=status  to surpress errors
   endif else begin
      yfitD = curvefit(X, deriv_newF, 1./Y, double(C), function_name='myfunc2_cor2', /noder, itmax=150, tol=tol,/double,status=status) ; add  ,status=status  to surpress errors
   end
   if status lt 1 then begin
      statstr = ' did not fit, status' 
      ++badfitscounter2
   endif else begin
      statstr = ' fit status'
   end
   if debug then print,'debug: done fit 2 for row ',t,statstr,status

   ; Antunes question: should 1./Y be 1./deriv_newF?
   ;    Answer: no, fit is every worse with that
   ;  Antunes-- no difference with mpcurvefit using 1./Y or deriv(1./Y)
   ;deriv_y = deriv(1./Y)
   ;yfitD = mpcurvefit(X, deriv_newF, deriv_y, C, function_name='myfunc2_cor2', /noder, itmax=150, tol=1.e-26,/double)
   ; Antunes: with no weighting, fewer errors but choppier fit
   ;yfitD = curvefit(X, deriv_newF, y*0, C, function_name='myfunc2_cor2', /noder, itmax=150, tol=1.e-26,/double)
   ; Antunes: weighting=1 i.e. absolutely trust the data, result same as default
   ;yfitD = curvefit(X, deriv_newF, (y*0)+1., C, function_name='myfunc2_cor2', /noder, itmax=150, tol=1.e-26,/double)

				;---- POSTER: yfitD ---> z[0]

      yyDum = deriv_newF/yfitD

      yyDum_extended = extendProfile(yyDum)
      yyWav = atrous_filter( yyDum_extended, 1./sqrt(yyDum_extended),nsig=2.0,/smear)    ; sigma=2
				;---- POSTER: yy ---> beta

    ; dynamic sizing, was yyWav[1024:2047]
    sx=(size(yfitD))[1]; typically something like 1024, 512, 720, 360, etc
    yyWav = yyWav[sx:sx+sx-1]

     ;   x1 = findgen(40)          ; Seamless patch!!!!!!!!!!
     ;   gg = poly_fit(x1, yyDum[0:39], 2, yfit = yfitDum)
     ; Antunes: no idea why this is here in Carlos' HI-1 original
     ; Was 0:39 for 0:1023 orig data
     ; so using that ratio, should be patch size given below.
    patch = floor(sx*0.04)-1
    yyWav[0:patch] = yyDum[0:patch]
    yy = yyWav  
    newyfitD = yfitD * yy	;---- POSTER: newyfitD ---> z[1]


; Integration of z[1]:
   Gx = newyfitD    
   GGx = reverse(Gx)
   for j=0,length-1 do xR[j] = total(GGx[j:length-1],/nan) +newF[0]   ;+ median((newF)[0:10])  
   xR = reverse(xr)		;---- POSTER: xR ---> I[2]
     



; ---- STEP 3:

   yToSmooth = newF / Xr
   fac = atrous_filter( yToSmooth, sqrt(yToSmooth),nsig=1.0,/smear)          ; sigma=1
				;---- POSTER: fac ---> f

   model[*, t] = xR * fac  	; POSTER ---> BIP   

   ;A = R
ENDFOR

status = 'Eq 1 did not fit '+strtrim(badfitscounter1,2)+','
status += ' Eq 2 did not fit '+strtrim(badfitscounter2,2)+' out of '+strtrim(nRows,2)+' rows'
print,status

if diag then device,decomposed=0 ; needed for tvscl colormap; thurs

RETURN, model    ;[ [[model]], [[model1]] ]
END

FUNCTION settitle,date,sc,firstorder=firstorder, minradii=minradii,cscale=cscale,tol=tol,polar=polar,mpfit=mpfit

     ; Antunes, build a fancy descriptive title
     title = 'COR2' + sc + ' ' + date + ' Fit '
     if keyword_set(firstorder) then title += 'Eq 1 only ' else title += 'Eq 1&2 '
     if keyword_set(minradii) then title += 'radii > ' + strtrim(minradii,2) + ' '
     if keyword_set(mpfit) then title += 'using mpcurvefit ' else title += 'using curvefit '
     if keyword_set(cscale) then title += 'scaling C by ' + strtrim(cscale,2) + ' '
     if keyword_set(tol) then title += 'tol set to ' + strtrim(tol,2)
     if keyword_set(polar) then title += 'polarfit '

     return,title
END

FUNCTION format_fullframe,img1,left=left,right=right
    ; not polar, but normal frame
  img_full = img1
        ; setting half-field here, for full frame just ignore
  xmidpt = ((size(img_full))[1])/2
  if keyword_set(right) then begin
     ; new try, actually decrease image size
     img_full=img_full[0:xmidpt-1,*]
  end else if keyword_set(left) then begin
     ; new try, actually decrease image size
     img_full=img_full[xmidpt:*,*]
     ; rectification to handle algorithm so sun is on the right
     img_full=reverse(img_full,1)
  end
  img1_go=img_full
  return,img1_go
END

; ==========================================================================
; ================================ MAIN PROGRAM ============================
; ==========================================================================

PRO cor2Proc_wModel, date = date, sc=sc, _extra=_extra,filter=filter,select_files=select_files,display=display,minradii=minradii,firstorder=firstorder,mpfit=mpfit,tol=tol,cscale=cscale,debug=debug,polar=polar,right=right,left=left, p1=p1, p2=p2, outerfilter=outerfilter, pmask=pmask, diag=diag, wispr=wispr

;+
; NAME:
;   COR2PROC_WMODEL
; PURPOSE:
;   Fits the Stenborg F-corona model to Cor2 data to create a better
;   background file
;
; CALLING SEQUENCE:
;     cor2proc_wmodel, date = '20131102', sc='B'
;
; INPUTS:
;    DATE -- string 'YYYYMMDD'
;    SC -- string 'A' or 'B'
;
; KEYWORD PARAMETERS
;     minradii=[n] filters out all data below the given radii, for better
;         algorithm convergence by ignoring the coronagraph-covered regions
;     /display shows pre- and post-processed images for validation
;     /polar = transforms into polar space first
;     /right = fits only right side
;     /left = fits only left side
;     /firstorder does just eq 1, not eq 2 (for bad convergence cases)
;     outerfilter = 0.xx sets outer circular mask to ignore beyond
;     (untested legacy code) /SELECT_FILES: pront the user to select the
;         number of desired files from a list (more than one can be selected)
;     (untested legacy code) FILTER: A FILENAME FILTER CAN BE DEFINED.
;         IF NOT DEFINED, THE STANDART VALUE IS SET: '*s4h1'+sc+'*.fts'
;     /wispr = resets dimensions to work for wispr data
;     /diag = adds extra intrusive diagnostic plots for peeking at the fit
;
; OUTPUTS:
;    saves the model background created to $COR2/bkgModels_[sc]
;
; DEPENDENCIES:
; .rn bargacr1/HI-1/background_creation/fan.pro 
; .rn bargacr1/HI-1/background_creation/cmapply.pro
; .rn bargacr1/HI-1/background_creation/atrous.pro
; .rn bargacr1/HI-1/background_creation/atrous_cont.pro
; .rn bargacr1/HI-1/background_creation/atrous_filter.pro
; .rn bargacr1/HI-1/background_creation/extendProfile.pro
;
; MODIFICATION HISTORY:
;    based on Braga's Hi1Proc_wModel routine
;    Mar 2021: S. Antunes (sandy.antunes@jhuapl.edu) modified
;    to work for Cor2
;
;-

 spacecraft=''
  IF not keyword_set(date) THEN begin
    date=''
    read,prompt='Date (YYYYMMDD): ',date
  ENDIF
  IF not keyword_set(sc) THEN begin
    read,prompt='Spacecraft (A or B): ',spacecraft
    sc=spacecraft
  ENDIF
  sc=strupcase(sc)
  standard_filename_filter='*s4h1'+sc+'*.fts'
  if not keyword_set(filter) then filter=standard_filename_filter
  ;;dirLoad = getenv('SECCHI_LZ')+'/L0/' + strlowcase(sc) + '/img/hi_1/'
  dirLoad = getenv('SECCHI_LZ')+'/L0/' + strlowcase(sc) + '/img/cor2/' ;; Antunes

  if not keyword_set(display) then display=0
  if not keyword_set(diag) then diag=0

  ;;files =
  ;;scclister(date,'HI_1',EXP='1190..1200',OSNUM='1516..1518',sc=sc,size='1024..1024',/AORB)
  
;;; Antunes removed scclister and went with file_search for background files
;;;  files = scclister(date,'COR2',sc=sc,size='2048..2048',/AORB) ;; Antunes

    basedir = getenv('SSWDB') + '/stereo/secchi/backgrounds/a/monthly_min/'

    if keyword_set(wispr) then begin
       date='wispr'
       files = file_search(basedir+date,'psp*.fits',/FULLY_QUALIFY_PATH)
       print,basedir+date
       print,files
    end else begin
       files = file_search(basedir+date,'mc2A_pTBr*.fts',/FULLY_QUALIFY_PATH)
    end

   ;files='/homes/antunak1/cor2_misc/20120602_010915_n4c2A.fts'; TUES

;;; Antunes removed for background files
;;;    if strlowcase(sc) eq 'a' then files=files.sc_a
;;;    if strlowcase(sc) eq 'b' then files=files.sc_b
    ;stop
    selected_ids=[]
    if keyword_set(select_files) then begin
      for i=0,n_elements(files)-1 do print,i,' ',files[i],' ',i
      file_id=-2
      while file_id ne -1 do begin
        read,prompt='Enter the desired number(s): ',file_id
        if file_id ne -1 then selected_ids=[selected_ids,file_id]
      endwhile
      ;stop
      files=files[selected_ids]
    endif

  ; Antunes test... just use a handful of files for now, for speed
  ;; files=files[0:5] ; hack, remove
  
  ;;dirSave_without_date=getenv('HI_1')+'/bkgModels_' + sc  +'/'
  dirSave_without_date=getenv('COR2')+'/bkgModels_' + sc  +'/' ; Antunes
  dirSave = dirSave_without_date + date 
  dir_ok=file_test(dirSave,/DIRECTORY)
  if dir_ok eq 0 then begin
    print,'Ouput base directory :'+dirSave+' does not exist. Trying to create it...'
    file_mkdir,dirSave
    print,'Ouput base directory :'+dirSave+' created successfully.'
  endif
  ;;spawn, 'mkdir ' + dirSave 
  print,'The following files will be processed by secchi_prep now: '
  print,files

  ; Antunes, window big enough for 2: 1 for img, 1 for polar img
  if display then window,/free,xsize=1500,ysize=1500
  
  ;;secchi_prep, files[0:*], h, cube,/fill_mean ; fill_mean takes cares of the missing blocks
  ;;secchi_prep, files[0:*], headers, cube, OUTSIZE=1024, /NOCALFAC, /CALIMG_OFF, /UPDATE_HDR_OFF, /EXPTIME_OFF ;; Antunes, removed
  ;;fill_mean
  ;;secchi_prep, files[0:*], headers, cube, OUTSIZE=1024, /CALIMG_OFF ;; Antunes for COR2 demo, removed opts


  ; Mar 22 2021 bug: secchi_prep used to work, now it returns junk

  cube=sccreadfits(files,headers) ; Antunes files mod
  ;cube=rebin(cube,1024,1024,1) ; TUES
  ; Carlos HI-1 original called seccchi_prep
  ;secchi_prep, files, headers, cube, /fill_mean

  ;;cube=cube*1e11 ; Antunes scaling added to prevent NaN
  ;stop
  ;;cube=hi_remove_saturation(cube,headers) ;; Antunes-removed for Cor2
  ; stop
  ;;help,cube
  ;;cube = sqrt(float(cube)*1e11) ;; Antunes-- removed now, put back
  ;;                                 in later once units are figured out

  ;cube[512:*,*,*]=0; block out half

  sz=size(cube) ;; Antunes
  print,'cube min/max/size:',min(cube),max(cube),sz

  n = n_elements(cube[0,0,*])

  if keyword_set(wispr) then begin
     bkgModel = fltarr(960,960,n) ; hard-coded hack for wispr tests
  end else begin
     if not keyword_set(polar) and (keyword_set(right) or keyword_set(left)) then $
        bkgModel = fltarr(sz[1]/2,sz[2],n) $
     else $
        bkgModel = fltarr(sz[1],sz[2],n)

  end

  FOR t = 0, n-1 DO BEGIN

     print, '**** Processing file:', t+1, n

     img0 = cube[*,*,t]      
     img1 = img0 ; copy to manipulate
     ;; Antunes, changed raw cube image to polar cube image
     wcs=fitshead2wcs(headers[t])
     center=wcs_get_pixel(wcs,[0,0])
     print,center
     ;center=[512.,512.] ; TUES

     zero_outer_circle,img1,center=center,outerfilter=outerfilter,minradii=minradii

     sz=size(img1) ; Antunes, making sizing dynamic not fixed at 1024
     maxval = max(img1)         ; save this for plotting normalization later
     medianval = median(img1)         ; save this for plotting normalization later

     if keyword_set(wispr) then begin
        ; horrible hack to get rid of to jerry-rig the wispr data in here
        ; uses incorrect sizing, so do not actually use!
        sz=size(img1)
        img1=rebin(img1,sz[1]/2,sz[2]/2)
        i2 = 1e11*img1[0:959,0:959]
        nani = where(~(FINITE(i2)))  ; removing NaN
        i2[nani]=0 ; removing NaN
        i3=rotate(i2,2)
        img1=i3
     end

     if sc eq 'B' then begin
         img1 = reverse(img1,1)
     endif

     title=settitle(date, sc, firstorder=firstorder, mpfit=mpfit, $
                      minradii=minradii,cscale=cscale,tol=tol,polar=polar)
     if display then begin
        xyouts, 10, 1460, title, charsize=2, /device
        plot4_display,img1,0,'Background file',/init
     endif

   ; --- check NaNs
     chkNan = finite(img1, /nan)
     idNan = where(chkNan eq 1, ct)
     ;stop
     if (ct gt 0) then img1[idNan] = median(img1) 
	 if (ct gt 10240) then print,'There are lots of nans';goto, skipit                     ; to avoid files with more than 10 cols of NaNs

     if keyword_set(polar) then begin
        ; convert to polar
        img1=format_polar(img1,center,pmask=pmask,trim=1)
     end else begin
        ; full frame
        img1=format_fullframe(img1,left=left,right=right)
     end

     sz=size(img1)
     img2 = [ img1, reverse(img1,1) ] 
     img2 = [  [reverse(img2,2)], [img2], [reverse(img2,2)] ]
     m1 = sigma_filter(img2, r=15, /iter,/all)
    ;;REPLACING THE BAD ROW (SATURATED DUE TO A PLANET) BY AN ADJACENT ONE
     ;;;;for i=0,sz[1]-1 do if min(m1[i,*]) eq max(m1[i,*]) then m1[i,*]=m1[i-1,*]
     ;;for i=0,sz[1]-1 do if min(img1[i,*]) eq max(img1[i,*]) then img1[i,*]=img1[i-1,*]
       ;;m1 = m1[0:1023,1024:2047]  
                                ; m1 is 3y size of img1, dupped 3 times.
                                ; Looks like Carlos wants middle?
     m1=img1; Antunes: m1 COR2 data filter is weird vis a vis HI-1

     if display then begin
        if keyword_set(polar) then label='Polar data' else label='Subsetted data'
        plot4_display,m1,1,label,polar=polar
     end

     model = img1

     print,'in ',min(img1),max(img1),median(img1)
     dummy = fitting(m1,img1, model = model, _extra=_extra, $
                     firstorder=firstorder, mpfit=mpfit, $
                     tol=tol,cscale=cscale, $
                     debug=debug, status=status, p1=p1, p2=p2, diag=diag)
     print,'out ',min(dummy),max(dummy),median(dummy)
     if sc eq 'B' then begin
       dummy = reverse(dummy,1)
    endif

     if display then begin
        if keyword_set(polar) then titlestr = 'Polar fit' $
        else titlestr = 'Fit after Eq 1 only'
        plot4_display,dummy,2,titlestr,polar=polar
     end

     invdummy = dummy
     if keyword_set(polar) then invdummy=unformat_polar(dummy,center)

     ;save,file='temp.sav',invdummy,img0 ; TUES
     ;help,dummy,invdummy,bkgModel
     bkgModel[*,*,t] = invdummy

     moreinfo = 'orig data max was '+strtrim(maxval,2)+' new fit max is '+strtrim(max(invdummy),2)
     print,moreinfo
     moremoreinfo = '     data median was '+strtrim(maxval,2)+'         median is '+strtrim(median(invdummy),2)
     print,moremoreinfo

     if display then begin
        plot4_display,invdummy,3,'Model fit'
        xyouts, 10, 30, moreinfo, charsize=2, /device
        xyouts, 10, 10, moremoreinfo, charsize=2, /device
        xyouts, 700, 10, status, charsize=2, /device
     end

     ;plot4_display,cube[*,*,t]-bkgModel[*,*,t],1,'Diff',polar=polar
     ;orig=cube[*,*,t]
     ;bck=bkgModel[*,*,t]
     ;save,file='temp.sav',orig,bck
     print,'Done ',title
     skipit:
     ;; Antunes, mod for shorter file names for background files
     ;;filename = dirSave + '/B_' + strmid(files[t], strlen(files[t])-25,25)
     stem=''
     if keyword_set(minradii) then stem = 'minradii_'+strtrim(minradii,2)+'_'

     filename = dirSave + '/B_' + stem + file_basename(files[t])
     sccwritefits, filename, bkgModel[*,*,t], headers[t]

     ;save,file='temp.sav',invdummy,cube; TUES
    
  ENDFOR

END




