import unittest
import test_objects

def test_suite():
  test_modules = [test_objects]
  loader = unittest.TestLoader()
  suite = unittest.TestSuite()
  for mod in test_modules:
    suite.addTest(loader.loadTestsFromModule(mod))
  return suite

