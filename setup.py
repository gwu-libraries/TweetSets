from setuptools import setup

if __name__ == "__main__":
    setup(
        name='TweetSets',
        version='2.0',
        url='https://github.com/gwu-libraries/TweetSets',
        author='GWU Libraries',
        author_email='sfm@gwu.edu',
        scripts=['tweetset_cli.py'],
        description='Archive tweets from the command line',
        install_requires=open('requirements.txt').read().split(),
        py_modules=['models'],
        packages = []
    )
