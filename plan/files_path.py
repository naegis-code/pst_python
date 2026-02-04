import pathlib

user_path = pathlib.Path.home()

if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

oustource_2025_file = filepath /'Report/2025/99 Plan/Outsource/Outsource & Vendor Y25 pieces & manday.xlsx'
oustource_2025_sheet = 'ฉบับใช้จริง Y25'
outsource_2025_skiprows = 3
outsource_2025_usecols = 'B:AQ'
outsource_2025_column_mapping = {'BUs.':'bu',
                                'Store Code':'stcode',
                                'Branch':'branch',
                                'Province':'province',
                                'HUB':'shub',
                                'Type':'st_type',
                                'CNTDATE':'cntdate',
                                'ประเภทการตรวจนับของ outsource':'cnt_type',
                                'Vendor':'outsource_name',
                                'Status':'hiring_status',
                                'Actual Cost (หัก miss rate แล้ว)':'outsource_expense',
                                'Van':'van_expense',
                                'Outsource (count pcs.)':'soh_outsource',
                                'cost per piece':'price_per_piece',
                                'Expire Cost':'expired_check',
                                'Est. Total (include Expire) ถ้ามี':'est_price_per_store',
                                'FOOD':'food_soh',
                                'NONFOOD':'nonfood_soh',
                                'PERISHABLE':'perishable_soh',
                                'Total SOH':'total_soh',
                                'Estimate Outsource':'est_man_total',
                                'Actaul Scan (pcs. หัก tag)':'qty_actual',
                                'Miss Rate (cost)':'missrate_actual',
                                'Print Out':'print_out_actual'}
outsource_2025_column_keep = ['bu','stcode','branch','province','shub',
                              'st_type','cntdate','cnt_type','outsource_name',
                              'hiring_status','outsource_expense','van_expense',
                              'soh_outsource','price_per_piece','expired_check',
                              'est_price_per_store','food_soh','nonfood_soh','perishable_soh',
                              'total_soh','est_man_total','qty_actual','missrate_actual',
                              'print_out_actual','est_man_food','est_man_nonfood','est_man_perishable',
                              'act_man_control','act_man_outsource']