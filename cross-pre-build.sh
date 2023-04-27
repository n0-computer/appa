set -e

arch=$CROSS_DEB_ARCH
# CROSS_DEB_ARCH is empty for android architectures.
if [ -z "$arch" ]; then
  if [ $CROSS_TARGET = "aarch64-linux-android" ]; then
    arch="arm64"
  elif [ $CROSS_TARGET = "armv7-linux-androideabi"]; then
    arch="armel"
  fi
fi
echo "building for target $CROSS_TARGET with debian architecture $arch"
dpkg --add-architecture $arch
apt-get update
apt-get install -y fuse:$arch libfuse-dev:$arch pkg-config clang
echo user_allow_other >> /etc/fuse.conf
