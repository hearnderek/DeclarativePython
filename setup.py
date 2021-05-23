import setuptools

with open("readme.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
     name='declarative',  
     version='0.0.0.1',
     author="Derek Hearn",
     author_email="hearnderek@gmail.com",
     description="Autoplumming python functions",
     long_description=long_description,
   long_description_content_type="text/markdown",
     url="https://github.com/hearnderek/DeclarativePython",
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )