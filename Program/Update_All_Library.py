import pkg_resources
import subprocess
import sys

def list_outdated_packages():
    result = subprocess.run(
        [sys.executable, "-m", "pip", "list", "--outdated", "--format=freeze"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("Failed to list outdated packages.")
        sys.exit(1)
    outdated_packages = [line.split("==")[0] for line in result.stdout.splitlines()]
    return outdated_packages

def upgrade_package(package_name):
    print(f"Upgrading {package_name} ...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade", package_name],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print(f"Successfully upgraded {package_name}.")
    else:
        print(f"Failed to upgrade {package_name}. Error: {result.stderr}")

def main():
    print("Upgrading pip...")
    subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])

    outdated_packages = list_outdated_packages()
    print(f"{len(outdated_packages)} packages outdated.")

    for package in outdated_packages:
        upgrade_package(package)

    print("All upgrades completed.")

if __name__ == "__main__":
    main()