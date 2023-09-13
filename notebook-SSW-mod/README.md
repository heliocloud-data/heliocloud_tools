# Jupyter SolarSoft modifications

This is the working repository for modifications for SolarSoft to enable work in Jupyter Notebooks.  The problem being solved is that Jupyter Notebooks do not allow the older plotting functions, instead requiring IDL object graphics calls; SolarSoft contains over 50,000 calls to the older routines.  The modifications include:
*   Pass-thru helper routines to reroute **plot, oplot, tv, tvscl,** and **xyouts** to the newer object-oriented routines.
*   A perl script that runs through a SolarSoft tree and replaces the above commands with their new **jnplot, jnplot,/overplot, jntv, jntv,/scale,** and **jnxyouts** replacements.
*   A walkthrough Jupyter Notebook that shows how to plot in Jupyter IDL, and does some elementary SSW-like operations.



## Contents

*   [Getting started](#getting-started)
*   [Approach and limitation](#approach-and-limitations)
*   [Install](#install)
*   [Sample cases](#sample-cases)
*   [Contribute](#contribute)
*   [License](#license)


## Getting started

There is a SSW build in /efs/solarsoft.  The notebook provides how to set the environment variable for accessing it, and a startup procedure you can modify to customize for your specific instrument loadouts required.

## Approach and limitations

The pass-thru approach uses an environmental variable 'SSWJUPYTER' that, if set, invokes the pass-thru object graphics instead of the older plot functions.  If SSWJUPYTER=0, then the pass-thru makes no changes and your SSW should work fine in terminal or EC2 usage.  If in Jupyter, setting SSWJUPYTER=1 enables the pass-thru to attempt the object orientated graphics.

The plotting pass-thru and perl script are tested and working.  The example SSW problems are still being tested.

Older Widgets (GUIs) do not work in Jupyter. This is because they rely on XWindows calls, which Jupyter disallows. Modifying widget programs like scclister() is not yet handled.

Not all plotting toggles are enabled, because the newer object oriented graphics uses different standards and expectations, for example requiring font objects rather than font names, and similar translation errors.  If there is significant demand, we will look at extending this further. We would appreciate feedback on further SSW items that do not work in Jupyter, so we can consider updates to this project.


## Install

Use git to clone this repository into your computer.

```
git clone https://git.smce.nasa.gov/heliocloud/heliocloud-services.git
```

## Sample cases

The attached notebook plots and overplots data, plots images, loads a FITS file, calls SPICE, does a sample background routine on WISPR data, and runs a CDAWeb example (not SSW per se but included as an example).

The intent is your existing code will work (but not look pretty) as long as you call the jn* equivalents of the older functions. We recommend that, for Notebook usage, to use the newer "p=plot()" and "i=image()" object graphics methods and cease using the older functions.


## Contribute

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[BSD-3](https://opensource.org/license/BSD-3-clause/)
