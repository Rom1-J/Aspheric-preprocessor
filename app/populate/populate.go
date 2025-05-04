package populate

import (
	"context"
	"github.com/Rom1-J/preprocessor/app/populate/logic"
	"github.com/Rom1-J/preprocessor/app/populate/structs"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	ucli "github.com/urfave/cli/v3"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Action(ctx context.Context, command *ucli.Command) error {
	logger.SetLoggerLevel(command)

	var (
		inputList []string

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving input descriptors
	// todo: dedupe code logic
	//
	inputFiles := command.StringSlice("input")
	inputDirectories := command.StringSlice("directory")
	searchRecursively := command.Bool("recursive")

	inputList = append(inputList, inputFiles...)

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() && !slices.Contains([]string{
				"00581409-2b4b-4b58-b6f8-05df5845061a",
				"01171936-f8e2-409b-b54a-dd715c480ec7",
				"0172a0f6-fc69-4b5d-aeba-d65bd2d03d6c",
				"033f9efb-f4cd-4418-9e43-1f361b5edd64",
				"06524c46-c57d-47a2-bd25-da49b24a064f",
				"093503fa-6ef9-4a41-80e7-407302e7050e",
				"0a63d2a0-6109-4f75-88ef-b8d36c923834",
				"0b591b67-2a2a-4a6c-ba29-861186eda679",
				"0c5a8855-8bb2-4fee-bf55-44c6ee7835e4",
				"0dccb5a7-f501-44ef-8d9a-4b5dba623d0d",
				"0f409026-f095-4b20-a6c6-4e909a1e3199",
				"10303438-e8ca-46d8-9066-4df6b2842ec8",
				"10c1566c-b5b1-4ffa-9443-a04d71d062e3",
				"10d1e335-5e08-4ebb-9c0a-952e57114808",
				"11db4fe6-0d65-4fd1-8cd4-f0533bb7b984",
				"14b1a645-f399-4a3b-b34b-8ff3dafc9425",
				"15e75876-b59f-442d-ad78-32bad288634f",
				"187a7b83-b2c2-40c1-b660-72c1713186b3",
				"1913fe03-44e8-4b6e-bffb-7ce721390cb5",
				"19b32ab1-7dd9-4882-9ed0-46b3f1951ae3",
				"19d0be76-d151-4757-af93-d63147bb4817",
				"19d608f9-6188-4b58-bb5b-75ec37ec6ee5",
				"19ff6e22-fb83-4319-bcfc-207799187deb",
				"1a11e13b-307f-4c85-852e-d991b99b2e25",
				"1f009899-d3b6-4c82-853e-44b7cad7bb10",
				"1f3a1eb0-a0cd-4a61-b5b7-9e048390d1dd",
				"1f5b9bde-4363-44dc-b418-65ee63b7de8c",
				"1ffcb01d-ad0c-435d-9360-28ee0129e6a2",
				"20673406-5ad3-4097-8a2a-23e4d2c37a16",
				"2078d482-c9c4-4ba2-8368-d84389d0b52f",
				"20a73860-e443-458e-94e0-47764f99abf4",
				"20bde60b-b2bc-4c1c-b374-e38a61c6e846",
				"21ab6dff-10aa-4a97-9c17-123ae9a733b6",
				"24233977-1c89-4f13-b509-a246fa718e34",
				"2524e9d1-cd1d-4c76-8863-417c75f6f666",
				"26cba2c7-175f-4257-969f-52ca8ed782b2",
				"27dbf04b-f5d0-4b9d-9b89-631d7069096f",
				"29fe4bde-b65a-4701-b4b8-79ffae7d4b26",
				"2a59055c-64b9-40d0-93ec-eda08f0c2d02",
				"2c9588ad-7472-4126-819c-af925405cba0",
				"2d9e3594-7c26-4da4-b557-47bb90c1fe29",
				"2da8a262-531d-43fa-91e7-b681e47ec58e",
				"2ef36f15-f2f4-42f3-bec5-25e4635b9461",
				"322cbabf-fd24-4782-b237-762c50939604",
				"32cf9661-34dd-49a4-a183-e49a0262e8b2",
				"331c6a34-b1cf-4716-ad53-49d6d8742ce0",
				"33a25bd9-6a06-4fb3-ae2d-de6831ae5832",
				"34fc6b76-db39-4b27-abea-a6aff5867f6c",
				"358d5eeb-e362-40a4-af26-7fcf22fed464",
				"35a8f365-3662-45c4-bc1d-8a464a1209e2",
				"3622d964-40f8-46d9-9963-bb9a9e047bc1",
				"3667abf0-4075-4f3c-a661-51893cc63165",
				"3682cd5b-172a-43ca-ae87-e5c00af1e23b",
				"373e914f-e429-4c50-9a4e-54dd425a4ec4",
				"39fa30d5-0724-47c6-9258-a1d6d711152c",
				"3b27b50c-a550-4bf8-8430-e0f204e2b271",
				"3c578439-5f70-457d-bf4d-c484d689c3f1",
				"3da87f18-d348-4ab0-a13f-e53c663a6343",
				"3e390d4b-d3cf-4106-a1b0-f55017122719",
				"3f6aa228-14e5-4a7c-9119-49f20350b458",
				"40f4bffc-cd25-4bfe-8739-1c0925a6eae0",
				"41c45efa-60f7-4f51-a6ca-e35e1ab63534",
				"43ef062b-94b4-4c9a-a163-64b78a9c9048",
				"46a94235-da59-4e64-ae40-9fdac51e396b",
				"46bf6466-3d25-421b-b3df-a80a8c5187ea",
				"476fd68e-3a01-4a4d-8a21-806a3ee0f663",
				"48013376-d165-41a2-ac94-9552c1c06975",
				"49f5cfff-a9a4-4549-885f-4da48d4774e6",
				"4b113c21-ae04-4ade-af94-fa92538ff3db",
				"4cc25dd3-8f24-4196-820b-686e28e5adfd",
				"4e686bde-dca8-4b68-b554-176cec84e9fe",
				"4ef8f720-3d65-49f9-97c2-b9338539e655",
				"5137f0a9-e757-4817-ac3d-a70e51363f14",
				"5153248d-e33b-4d4e-b3bc-1ff673f60cf3",
				"52b63416-e37e-410f-941d-af9ff750e81b",
				"5307d8e9-8ec7-46c7-89bc-04be4dbdc307",
				"53afe0bd-0c24-40f9-9c0c-4d68717dbdc1",
				"57dce920-ea87-442b-bfe0-6eb92526ccee",
				"594e9ab3-a4ed-43b2-bde7-e3399bbdeacd",
				"596e2447-724f-4f73-b2f2-5941343f287f",
				"597c58be-ad92-4002-a611-b1e477b09e69",
				"5a056963-a99b-43b3-86a4-e49bf15bd29e",
				"5cf41d8b-9121-4aca-a8a2-5456b145a6d0",
				"5eb6ee1a-7914-45c5-8b43-31518d43624a",
				"5f51ec0d-9559-4665-a67d-9552ef7b2b0b",
				"5f944ab9-3547-4ffe-a367-8c67819ae108",
				"614f88f5-3b51-43b5-833a-9289b34d8dbc",
				"6362d315-f6d1-4f27-9b28-556be0b5f21b",
				"63aec7a7-6db9-481e-a0da-77be434289e1",
				"65116629-6cab-4b4c-9723-c90ef6ef06d9",
				"67130c59-79d1-4900-9785-f095fbe06646",
				"678e0ba3-f85a-46d7-9ae0-4641c1673cd1",
				"67f95ca7-3211-4db8-9483-b278cffaef9d",
				"6952721e-0fe6-406c-afcd-1105e3bb2814",
				"6d4831f9-2ebe-4c66-af05-6ec17a69491f",
				"6d975660-5b14-4172-b6c6-47a1e2989347",
				"6d9ccee1-3a74-4b5e-a247-02c04604d001",
				"6eba6ce7-d3d2-46c7-913b-82aa788d4851",
				"70025529-a499-4e23-a06e-f37fedfb2537",
				"72c3bc4c-c72f-4b9e-beb6-91093bf582d0",
				"73a0bc2b-40a4-4248-bd6c-ed15de00ab8d",
				"73a20ae7-2563-40f9-bf45-01cb755678ea",
				"764e4536-9fde-46f8-8e01-edb9fa8a8721",
				"77d704cd-4f05-4e98-a822-ec687f921399",
				"77e859b0-5370-4cd2-821c-bbb09a521778",
				"78124770-2ad4-4499-941c-bcb44b882091",
				"792cc401-78d1-4845-aee4-3af32780d76f",
				"7965482b-ba12-41b2-a725-20cf239f6305",
				"7c4432aa-41c8-4008-ab27-4dcc53d1fb46",
				"7d0ba672-bf5b-4dfd-aecc-eac4cc255a06",
				"7d2a2137-9350-42b8-89c4-8403e43e74e7",
				"7ee1e501-2f58-41b0-8e30-64933016b3b0",
				"7f1e8a17-4e3c-4927-ac2f-9467d7b5251f",
				"7fa3d9e6-c98e-493e-91a8-ecef5929d475",
				"80982087-2984-44d7-bfb6-ff1172c78a48",
				"80b43c44-4936-4dbc-a0e9-06803c972a66",
				"80e40558-489f-4aba-bb0f-5891802a49fc",
				"83c3c5bb-7cca-4a5d-acb9-8116706552c5",
				"845d29ab-d573-4fe1-b653-386418697f9f",
				"8ac3a693-f7cb-48fe-809b-ede99ec0fac2",
				"8acc5771-a9b4-4320-bc93-555521cf7c13",
				"8d7ba262-7944-4b6a-946c-9361fea841a1",
				"8ebb4c8f-518e-441b-822c-91cd92cc6c87",
				"8f57719b-173e-4a14-a4b2-2e8742c2aa6a",
				"8f84ae90-771b-43c8-86f3-cb966c065044",
				"9155d060-f19c-46ba-9932-71fa8205a7b1",
				"92103f83-9e16-4a17-bbad-ca464918dd82",
				"92b4cc8c-63c6-49cf-a169-6f3a6340e225",
				"93049b6c-ba6e-4d96-b9aa-d8306dd89226",
				"933c9c71-56aa-4a10-b818-be7e68f35221",
				"938c11d8-ed69-4845-a560-34476ddea5e8",
				"943da04c-aa3d-4dc0-b5fe-75c82eb90283",
				"958d07e1-a732-4b62-9cca-a33eead24674",
				"967a1a39-37fb-4b0b-b526-1c93d2b16531",
				"984349a2-b6df-42c8-b848-32f5cd446af9",
				"99d361d1-912a-4bd7-a8d0-58697426e157",
				"9a2c725a-6e99-4161-a7b4-8f9ff22e8096",
				"9b6f36c6-6eb7-4529-a96c-5dc1befe8712",
				"9cb9bc3d-d271-43f2-a624-394dae86b851",
				"9d4108d1-2da5-467a-aba2-e704e153c607",
				"9d680f12-b6e9-4ec5-b9e3-7245054021db",
				"9e6f62cb-3939-49cc-aaa1-e4e341ce51ce",
				"9fb0cd63-20be-4d29-a31e-5ee2164fbe98",
				"a0372e0d-e87a-4ba7-982e-da70732c4059",
				"a0bb8d07-1fc0-40cd-beab-e0f34cdbb3f6",
				"a0dc9261-abb7-4213-81d5-9689060a8f81",
				"a2550118-6741-4610-b426-0f055006c530",
				"a2f43ff5-e0f8-4b66-88c1-4c34ed9d9274",
				"a3437e1e-4a96-4144-a560-35a4835a7e8d",
				"a690d0b4-a3f2-453f-990a-d05852e77790",
				"a6c64e02-b506-4546-9104-b3a3777aeb4f",
				"a707e101-bd45-4baf-bd7e-6cdd09817a7c",
				"a74bd61a-b093-4cab-80a7-cbe31e1f4a43",
				"a7797c89-a56a-4a29-b0f4-8783125c2d48",
				"a99ced67-3b8d-43dc-8bba-57e887f8c61c",
				"aa7fd643-3c2e-4a88-8d48-7f6b3d45ed2e",
				"ab18f854-04a0-479c-b16c-3ea95b05317c",
				"ad6dcfd2-10ce-4382-8a05-c487787fb32a",
				"adf24a31-ac11-499f-89c7-82e355d89b4d",
				"af6f341d-203f-4c1a-9b1b-4a000d321c9a",
				"b015e1c1-1e06-4be1-8230-2591f8af9093",
				"b0784f2e-f72c-451a-94be-b4ff76924a00",
				"b0af6e9e-45b3-44f9-b057-9e7c6ae9eea9",
				"b0db9bab-46c0-4200-b170-4ad7fdd1afae",
				"b18c1902-11b0-4338-90e5-b95dbff529ef",
				"b19a3abe-a26d-4283-b5bc-366177b17d00",
				"b269f95f-912f-48d9-82ad-c68bd99a3c61",
				"b36912e8-8be2-46b4-beba-36b179d7a6ab",
				"b3ef235f-787c-4939-aefd-3a6ed0c51874",
				"b4dd112b-2058-462f-8ecf-4b97cfe2a2cf",
				"b7306dae-9340-49c8-9f1a-ebda86b973bf",
				"b86d231a-8a29-4919-9bcb-61eebd6d26b6",
				"ba38799d-380b-4768-9322-298beed86ece",
				"bb286621-4463-4b1e-a16b-59a9b18ca54e",
			}, info.Name()) {
				metadataFilePath := filepath.Join(path, "_metadata.pb")
				if _, err := os.Stat(metadataFilePath); err == nil {
					inputList = append(inputList, path)
				}

				if !searchRecursively {
					return filepath.SkipDir
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	logger.Logger.Debug().Msgf("Input files: %v", inputList)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize progress bar
	//
	globalProgress := prog.New("Directories processed", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Processing paths
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for i, inputDirectory := range inputList {
		logger.Logger.Trace().Msgf("Locking slot for saving: %s", inputDirectory)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(ipd string) {
			defer func() {
				logger.Logger.Trace().Msgf("Releasing slot for saving: %s", ipd)

				globalProgress.GlobalTracker.Increment(1)
				<-semaphore
				wg.Done()
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Ingesting _metadata.pb
			//
			if err := logic.ProcessDirectory(
				globalProgress,
				ipd,
				structs.SolrOptsStruct{
					Address:    command.StringSlice("url")[i%len(command.StringSlice("url"))],
					Collection: command.String("collection"),
				},
			); err != nil {
				logger.Logger.Error().Msgf("Cannot ingest file '%s': %s", ipd, err)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}(inputDirectory)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	wg.Wait()

	logger.Logger.Info().Msg("Done!")

	return nil
}
