import xml.etree.ElementTree as ET

def cdawebxml2txt(input_xml="all.xml",output_txt="dataids.txt",writefile=True):
    # Parse XML
    tree = ET.parse(input_xml)
    root = tree.getroot()

    # Handle default namespace (xmlns="cdas")
    ns = {"cdas": root.tag.split("}")[0].strip("{")}

    # Find all <dataset> elements under any parent in this namespace
    datasets = root.findall(".//cdas:dataset", ns)
    allids = []
    # Write each dataset's ID attribute, one per line
    for ds in datasets:
        ds_id = ds.get("serviceprovider_ID")
        if ds_id:  # only if ID exists
            allids.append(ds_id)
    if writefile:
        with open(output_txt, "w", encoding="utf-8") as f:
            for ele in allids:
                f.write(ele + "\n")
    return allids

if __name__ == "__main__":
    cdawebxml2txt()
