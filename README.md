# NOTICE

### EVENTHOUGH REALDEBRID NOW SUPPORTS WEBDAV, I STILL RECOMMEND USING THIS FORK

Realdebrid has added support for the WebDav protocoll, which makes it mountable through official rclone software.
As of now (24.06) this webdav implementation does not support file deletion and is a whole lot slower than my fork. That is because each time a file is accessed through the webdav server, realdebrid unrestricts the file. If you refresh your plex library of the mounted drive for example, every single file is unrestricted again and again, which takes a very long time.

# RClone_RD

This RClone Fork contains a Real-Debrid implementation.
Using this version, the entire RealDebrid /torrents directory can be served as a read-only virtual drive. 

A potential use-case for this is serving the /torrent directory over plex, allowing you to build a media library truly unlimted in size. Im working on a project that allows plex to function the same way that Wako,Syncler and other streaming apps do. Check it out on https://github.com/itsToggle/plex_rd

### Capabilities and Limitations:

- Read/Write capabilities are limited to reading files and deleting them. 
- There are no server-side traffic limitations.
- This rclone fork will automatically re-activate direct links when they expire after 1 week.
- There is a server-side connection limit, which I believe is 16 parallel connections.

## Installation:

For Linux and Mac OSX, I will be providing cross-compiled releases, which I **cannot test**. Please feel free to test them out.

### Windows:

- install winfsp (https://winfsp.dev/)
- download the latest pre-built 'rclone.exe' file from here: https://github.com/itsToggle/rclone_RD/releases

### Mac OSX (untested build):

- download the latest pre-built 'rclone-darwin' file from here: https://github.com/itsToggle/rclone_RD/releases

### Linux (untested build):

- download the latest pre-built 'rclone-linux' file from here: https://github.com/itsToggle/rclone_RD/releases

### Android:
This version is based the latest release from the [rcx github](https://github.com/x0b/rcx). Ive simply replaced the 'librclone.so' file inside the apk with a compiled version of my rclone fork. To be able to install the apk, the app needed to be signed again which i have done with an [apk-signer app](https://play.google.com/store/apps/details?id=com.haibison.apksigner&hl=en&gl=US) from the android playstore, which Im pretty sure isnt malicious. 
To add realdebrid as a remote, simply setup a realdebrid remote on your PC and copy the `rclone.conf` file (`C:\Users\BigSchlong\.config\rclone`) to your android device. Inside the RCX App you can now load your rclone config file. Youre done, you can mount realdebrid on your android device :)

- download the latest pre-built 'apk' file from here: https://github.com/itsToggle/rclone_RD/releases

## Setting up the remote (Every OS except android):

1. create a new remote using: 'rclone config' - Depending on your OS this command might have to be '.\rclone config'
2. choose a name for your remote e.g. 'your-remote'
3. choose 'realdebrid' ('47')
4. enter your realdebrid api key (https://real-debrid.com/apitoken)
6. choose 'no advanced config'
7. your RealDebrid remote is now set up.
8. Mount the remote 'rclone cmount your-remote: your-destination:' - replace 'your-remote' with your remotes name. replace 'your-destination:' with a drive letter, e.g. 'X:' See the recommended tags section to speed up rclone!
9. Enjoy!

### Recommended Tags when mounting:

It is recommended to use the tags in this example mounting command: 

'rclone cmount torrents: Y: --dir-cache-time=10s --vfs-cache-mode=full'

This will significantly speed up the mounted drive and detect changes faster.

## Building it yourself (Windows)

I really do suggest downloading the pre-built release. But if you want to tinker a bit and built it yourself, here are the steps:
- Download the project files. 
- Install Golang
- To build the project, you need to have MinGW or a different cgo adaptation installed.
- install WinFsp.
- If you dont want to mount the remote as a virtual drive but rather as a dlna server or silimar, use 'go build' to build the project.
- If you do want to mount the remote as a virtual drive, continue:
- Build the project using 'go build -tags cmount'. 
- if that fails on 'fatal error: fuse_common.h missing', you need to do the following steps:
- Locate this folder: C:\Program Files (x86)\WinFsp\inc\fuse - inside you will find the missing files.
- Copy all files to the directory that they are missing from. For me that was: C:\Users\BigSchlong\go\pkg\mod\github.com\winfsp\cgofuse@v1.5.1-0.20220421173602-ce7e5a65cac7\fuse
- Try to build it again

## Building it yourself (Mac/Linux)

- Download the project files
- Install Golang 
- Run a terminal in the root directory of the project files
- use 'go build -tags cmount' to build the project
- If anything fails, Check the official rclone Channels for Help.
- Please feel free to contact me if you have compiled a version, so I can provide it as a comunity build for others :)


