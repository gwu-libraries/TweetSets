from setuptools import setup

if __name__ == "__main__":
    setup(
        name='TweetSets',
        version='0.1',
        url='https://github.com/justinlittman/TweetSets',
        author='Justin Littman',
        author_email='justinlittman@gmail.com',
        scripts=['tweetset_cli.py'],
        description='Archive tweets from the command line',
        install_requires=open('requirements.txt').read().split(),
        py_modules=['models'],
        packages = []
    )
