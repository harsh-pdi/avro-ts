import { existsSync, mkdirSync, readFileSync } from 'fs';
import { createAvroFile, readAvroFile, updateAvroFile } from './avro';
import { messageSchema } from './schema';

const TMP_DIR = __dirname + '/tmp/';
const MESSAGES_FILE_1 = __dirname + '/data/messages.json';
const MESSAGES_FILE_2 = __dirname + '/data/messages2.json';
const NEW_MESSAGE_FILE_NAME = TMP_DIR + `${Date.now()}-messages.avro`;

const getMessages = async (filePath: string) => {
    return await readFileSync(filePath, { encoding: 'utf-8' });
}

const processMessages = async () => {
    try {
        if (!existsSync(TMP_DIR)) {
            await mkdirSync(TMP_DIR);
        }
        const messages = JSON.parse(await getMessages(MESSAGES_FILE_1));
        const messages2 = JSON.parse(await getMessages(MESSAGES_FILE_2));
        await createAvroFile(messages, NEW_MESSAGE_FILE_NAME, messageSchema);
        await updateAvroFile(messages2, NEW_MESSAGE_FILE_NAME, messageSchema);
    } catch (err) {
        console.error(err);
    }
}

const readMessages = async () => {
    return await readAvroFile(NEW_MESSAGE_FILE_NAME);
}

(async() => {
    await processMessages();
    const messages = await readMessages();
    console.log(messages);
})();
