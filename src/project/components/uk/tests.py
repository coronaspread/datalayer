
import os
import os . path
import subprocess

import ukdl
import ukglobals

assert (os . path . exists (ukglobals . country_indicators_file_name))
os . remove (ukglobals . country_indicators_file_name)
  

assert (os . path . exists (ukglobals . regional_confirmed_cases_file_name))
os . remove (ukglobals . regional_confirmed_cases_file_name)

try :
  ukdl . download_the_data ("nothin")
except (subprocess . CalledProcessError) :
  print ("all good")
else :
  assert (False)

