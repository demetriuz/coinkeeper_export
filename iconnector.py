#!/usr/bin/python
import subprocess
import os

def ifuse_connect(mnt_path='/tmp/ios', app_id='com.companyname.appname'):
    try:
        os.mkdir(mnt_path)
    except OSError:
        pass
    subprocess.call('ifuse {mnt_path} --appid {app_id}'.format(**locals()), shell=True)

def ifuse_disconnect(mnt_path='/tmp/ios'):
    res = subprocess.call('umount {mnt_path}'.format(**locals()), shell=True)
    os.rmdir(mnt_path)

if __name__ == '__main__':
    ifuse_connect()

#sudo cp -rfX /usr/local/Cellar/fuse4x-kext/0.9.1/Library/Extensions/fuse4x.kext /System/Library/Extensions
#sudo chmod +s /System/Library/Extensions/fuse4x.kext/Support/load_fuse4x
#kextunload /System/Library/Extensions/fuse4x.kext/
#sudo kextload /System/Library/Extensions/fuse4x.kext/