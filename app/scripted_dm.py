"""Finite, explicit natural-language templates. This is NOT an LLM/NLP parser."""
from .actors import ActorHost
from .world import WorldError
import re

WAIT = '本轮暂候消息，不采取新的行动。'
INVESTIGATE = '核查本地粮账与现场状况，等待实际调查完成。'


class DefaultActors(ActorHost):
    @staticmethod
    def _choose(view, seen):
        body, consumed = ActorHost._choose(view, seen)
        if body['action'] == 'wait':
            if view['person']['office'] in ('finance', '内政财政官'):
                ledger = next((re.search(r'粮\s*(\d+)', item['text']) for item in reversed(view['observations'])
                               if '核账' in item['text'] and re.search(r'粮\s*(\d+)', item['text'])), None)
                for message in view['messages']:
                    key = 'message:'+message['id']
                    target = re.search(r'我在(.+?)亲见', message['text'])
                    if (key not in seen and 'ledger:'+message['id'] in seen and ledger and int(ledger[1]) >= 10
                            and target and target[1] != view['person']['location']):
                        text = f'向来信提到的「{target[1]}」调运10份本地粮，并向君主报告此次已发起、尚未到达的运输。'
                        return {'action':'propose', 'text':text}, consumed+[key]
            return {'action': 'propose', 'text': WAIT}, consumed
        if body['action'] == 'investigate':
            text = INVESTIGATE
        elif body['action'] in ('send', 'command'):
            contacts = [p for p in view['contacts'] if p['id'] == body['to']]
            if len(contacts) != 1:
                raise WorldError('UNSUPPORTED', '脚本收件人缺少公开姓名')
            verb = '报信' if body['action'] == 'send' else '发出命令'
            text = f"向「{contacts[0]['name']}」{verb}：{body['text']}"
        elif body['action'] == 'respond':
            index = next(i for i, message in enumerate(view['messages']) if message.get('command_id') == body['command_id'])
            text = f"我选择{'接受' if body['accept'] else '拒绝'}实际收到的第{index+1}封来信中的命令。"
        elif body['action'] == 'ship':
            index = next((i for i, message in enumerate(view['messages'])
                          if (match := re.search(r'(?:目的地|地点)[：:]\s*([^\s，。；;]+)', message['text']))
                          and match[1] == body['to']), None)
            if index is None:
                raise WorldError('UNSUPPORTED', '调运目的地缺少本人来信来源')
            text = f'按实际收到的第{index+1}封来信指定的目的地调运10份本地粮，并向君主报告已发起运输。'
        else:
            raise WorldError('UNSUPPORTED', '未实现的自然语言脚本动作')
        return {'action': 'propose', 'text': text}, consumed


class ScriptedDM:
    def __init__(self, world):
        self.world = world

    @staticmethod
    def _shipment(view, state, target_id):
        targets = [p for p in state['places'] if p['id'] == target_id and p['country_id'] == view['person']['country_id']]
        kings = [p for p in view['contacts'] if p['office'] in ('king', '君主')]
        if len(targets) != 1 or len(kings) != 1:
            raise WorldError('UNSUPPORTED', '目的地或收件人不明确')
        return [{'action':'ship', 'to':target_id, 'quantity':10},
                {'action':'send', 'to':kings[0]['id'], 'text':'我已发起赈济运输；发信时粮尚未到达。'}]

    def __call__(self, context):
        proposal, state = context['proposal'], context['world']
        text = proposal['text']
        view = self.world.observe(proposal['person_id'])
        if text == WAIT:
            effects = [{'action': 'wait'}]
        elif text == INVESTIGATE:
            effects = [{'action': 'investigate'}]
        elif (match := re.fullmatch(r'向来信提到的「([^」]+)」调运10份本地粮，并向君主报告此次已发起、尚未到达的运输。', text)):
            if not any(f'我在{match[1]}亲见' in item['text'] for item in view['messages']):
                raise WorldError('UNSUPPORTED', '目的地没有实际来信来源')
            targets = [p for p in state['places'] if p['name'] == match[1] and p['country_id'] == view['person']['country_id']]
            if len(targets) != 1:
                raise WorldError('UNSUPPORTED', '目的地姓名不明确')
            effects = self._shipment(view, state, targets[0]['id'])
        elif (match := re.fullmatch(r'按实际收到的第([1-9][0-9]*)封来信指定的目的地调运10份本地粮，并向君主报告已发起运输。', text)):
            index = int(match[1])-1
            messages = view['messages']
            target = re.search(r'(?:目的地|地点)[：:]\s*([^\s，。；;]+)', messages[index]['text']) if index < len(messages) else None
            if target is None:
                raise WorldError('UNSUPPORTED', '调运缺少真实已收信件的目的地')
            effects = self._shipment(view, state, target[1])
        elif (match := re.fullmatch(r'向「([^」]+)」(报信|发出命令)：(.+)', text, re.DOTALL)):
            targets = [p for p in view['contacts'] if p['name'] == match[1]]
            if len(targets) != 1:
                raise WorldError('UNSUPPORTED', '收件人姓名不明确')
            effects = [{'action': 'send' if match[2] == '报信' else 'command', 'to': targets[0]['id'], 'text': match[3]}]
        elif (match := re.fullmatch(r'我选择(接受|拒绝)实际收到的第([1-9][0-9]*)封来信中的命令。', text)):
            index = int(match[2])-1
            messages = view['messages']
            if index >= len(messages) or not messages[index].get('command_id'):
                raise WorldError('UNSUPPORTED', '对应命令尚未实际送达')
            effects = [{'action':'respond', 'command_id':messages[index]['command_id'], 'accept':match[1]=='接受'}]
        else:
            raise WorldError('UNSUPPORTED', '未识别的有限脚本模板')
        return {'world_id': state['world_id'], 'base_revision': state['revision'],
                'ruling': '成功', 'reason': '有限脚本DM按明确模板裁定；不是实际模型理解。',
                'goal': proposal['text'], 'costs': [], 'benefits': [], 'effects': effects}
