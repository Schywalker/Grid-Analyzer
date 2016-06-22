#!/usr/bin/env python

## Change Log
# Mar 7 2016- Added show_progress and update_bar function

import sys
import time
    
#display progress on terminal window
def show_progress(counter, total_lines):  
  pcnt_completed = round(float(counter)/float(total_lines)*100.0,1)
  update_bar(pcnt_completed)

#update progress status
def update_bar(status):
    sys.stdout.write("\r%d%% completed..." % status)
    sys.stdout.flush()

class utilities:

  global levelMsg
  levelMsg =  {1:'TRACE', 2:'DEBUG', 3:'INFO'}
  
  def __init__(self, logLevel):
    self._logLevel = logLevel
    self._indent = 0

  def logit(self, level, msg):
    if level >= self._logLevel:
      self.inc_indent(level)
      print self.makedent() + levelMsg[level] + ':', msg
      self.dec_indent(level)


  def makedent(self):
    return ' ' * self._indent

  def inc_indent(self, level):
    self._indent = self._indent + 2*(3-level)
    
  def dec_indent(self, level):
    self._indent = self._indent - 2*(3-level)