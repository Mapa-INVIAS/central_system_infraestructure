#!/usr/bin/env python
import sys
from pathlib import Path

def parse_packages(txt_path):
    packages = []
    with open(txt_path, "r", encoding="utf-16", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Ignorar paquetes internos/binarios
            if line.startswith("_") or line.startswith("lib") or line.startswith("vc"):
                continue

            # Formato: nombre=version=build
            parts = line.split("=")
            if len(parts) >= 2:
                name = parts[0]
                version = parts[1]

                # Filtrar runtime y toolchain
                skip_prefixes = (
                    "vs2015", "vc14", "ucrt", "m2w64", "mingw",
                    "xorg-", "font-", "fonts-", "ca-certificates"
                )
                if name.startswith(skip_prefixes):
                    continue

                packages.append(f"{name}={version}")

    return sorted(set(packages))


def write_environment_yml(packages, output_path, env_name):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"name: {env_name}\n")
        f.write("channels:\n")
        f.write("  - conda-forge\n")
        f.write("  - defaults\n\n")
        f.write("dependencies:\n")

        has_pip = False
        for pkg in packages:
            if pkg.startswith("pip="):
                has_pip = True
            f.write(f"  - {pkg}\n")

        if not has_pip:
            f.write("  - pip\n")


def main():
    if len(sys.argv) < 3:
        print("Uso: python convert_txt_to_yml.py input.txt output.yml [nombre_entorno]")
        sys.exit(1)

    input_txt = Path(sys.argv[1])
    output_yml = Path(sys.argv[2])
    env_name = sys.argv[3] if len(sys.argv) > 3 else "inviasvivo"

    if not input_txt.exists():
        print(f"Error: no existe el archivo {input_txt}")
        sys.exit(1)

    packages = parse_packages(input_txt)
    write_environment_yml(packages, output_yml, env_name)

    print(f" environment.yml generado: {output_yml}")
    print(f" Paquetes incluidos: {len(packages)}")
    print(f" Nombre del entorno: {env_name}")


if __name__ == "__main__":
    main()
