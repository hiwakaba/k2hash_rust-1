#!/bin/sh
#  
# k2hash_rust
#
# Copyright 2025 LY Corporation.
# 
# Rust driver for k2hash that is a NoSQL Key Value Store(KVS) library.
# For k2hash, see https://github.com/yahoojapan/k2hash for the details.
#  
# For the full copyright and license information, please view
# the license file that was distributed with this source code.
#  
# AUTHOR:   Hirotaka Wakabayashi
# CREATE:   Fri, 17 Jul 2025
# REVISION:                        
#  

echo $(basename $0)

if test -f "/etc/os-release"; then
    . /etc/os-release
    OS_NAME=$ID
    OS_VERSION=$VERSION_ID
elif test -f "/etc/centos-release"; then
    echo "[OK] /etc/centos-release falling back to CentOS-7"
    OS_NAME=centos
    OS_VERSION=7
else
    echo "Unknown OS, neither /etc/os-release nor /etc/centos-release"
    exit 1
fi

echo "[OK] HOSTNAME=${HOSTNAME} OS_NAME=${OS_NAME} OS_VERSION=${OS_VERSION}"

case "${OS_NAME}-${OS_VERSION}" in
    ubuntu*|debian*)
        DEBIAN_FRONTEND="noninteractive" sudo apt-get update -y
        DEBIAN_FRONTEND="noninteractive" sudo apt-get install -y curl
        curl -s https://packagecloud.io/install/repositories/antpickax/stable/script.deb.sh | sudo bash
        DEBIAN_FRONTEND="noninteractive" sudo apt-get install -y k2hash-dev
        ;;
    centos-7)
        sudo yum install -y epel-release-7
        sudo yum install -y --enablerepo=epel git curl which
        curl -s https://packagecloud.io/install/repositories/antpickax/stable/script.rpm.sh | sudo bash
        sudo yum install -y k2hash-devel
        ;;
    centos-8|fedora*)
        sudo dnf install -y epel-release-8
        sudo dnf install -y git curl
        curl -s https://packagecloud.io/install/repositories/antpickax/stable/script.rpm.sh | sudo bash
        sudo dnf install -y k2hash-devel
        ;;
esac

exit $?

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
